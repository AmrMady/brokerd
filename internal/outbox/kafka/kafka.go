// Package kafka publishes outbox events to Kafka and consumes them. It
// uses confluent-kafka-go, which wraps librdkafka and needs cgo, so it's
// kept out of the brokerd binary.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AmrMady/brokerd/internal/outbox"
)

// Topic is where events of a type go: the prefix and the type, like
// brokerd.entry_posted.
func Topic(prefix, typ string) string {
	return prefix + typ
}

// Publisher publishes outbox events to Kafka. Events are keyed by account,
// so an account's events land on one partition, in order. The producer is
// idempotent, so its own retries can't duplicate or reorder them.
type Publisher struct {
	producer *ckafka.Producer
	prefix   string
	done     chan struct{}
}

// NewPublisher connects to brokers.
func NewPublisher(brokers, prefix string, logger *slog.Logger) (*Publisher, error) {
	p, err := ckafka.NewProducer(&ckafka.ConfigMap{
		"bootstrap.servers":   brokers,
		"enable.idempotence":  true, // acks=all, and retries keep partition order
		"compression.type":    "zstd",
		"linger.ms":           5,
		"delivery.timeout.ms": 30000,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	pub := &Publisher{producer: p, prefix: prefix, done: make(chan struct{})}
	// Delivery reports go to Publish; this gets the producer's other
	// events, like lost connections.
	go func() {
		defer close(pub.done)
		for ev := range p.Events() {
			if kerr, ok := ev.(ckafka.Error); ok {
				logger.Warn("kafka producer", "error", kerr)
			}
		}
	}()
	return pub, nil
}

// EnsureTopics creates the topics for the event types, unless they exist.
func (p *Publisher) EnsureTopics(ctx context.Context, types []string, partitions, replication int) error {
	admin, err := ckafka.NewAdminClientFromProducer(p.producer)
	if err != nil {
		return err
	}
	defer admin.Close()
	specs := make([]ckafka.TopicSpecification, len(types))
	for i, t := range types {
		specs[i] = ckafka.TopicSpecification{Topic: Topic(p.prefix, t), NumPartitions: partitions, ReplicationFactor: replication}
	}
	results, err := admin.CreateTopics(ctx, specs)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for _, r := range results {
		if r.Error.Code() != ckafka.ErrNoError && r.Error.Code() != ckafka.ErrTopicAlreadyExists {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Error)
		}
	}
	return nil
}

// Publish implements outbox.Publisher: it sends the events and waits until
// Kafka has acknowledged all of them.
func (p *Publisher) Publish(ctx context.Context, events []outbox.Event) error {
	deliveries := make(chan ckafka.Event, len(events))
	for _, e := range events {
		value, err := json.Marshal(e)
		if err != nil {
			return err
		}
		topic := Topic(p.prefix, e.Type)
		err = p.producer.Produce(&ckafka.Message{
			TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
			Key:            []byte(e.Key),
			Value:          value,
			Headers: []ckafka.Header{
				{Key: "event_id", Value: []byte(e.ID)},
				{Key: "event_type", Value: []byte(e.Type)},
			},
		}, deliveries)
		if err != nil {
			return fmt.Errorf("produce event %s: %w", e.ID, err)
		}
	}
	var errs []error
	for range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev := <-deliveries:
			if m, ok := ev.(*ckafka.Message); ok && m.TopicPartition.Error != nil {
				errs = append(errs, m.TopicPartition.Error)
			}
		}
	}
	return errors.Join(errs...)
}

// Close sends what's queued, for up to five seconds, and disconnects.
func (p *Publisher) Close() {
	p.producer.Flush(5000)
	p.producer.Close()
	<-p.done
}

// ConsumerConfig configures Consume.
type ConsumerConfig struct {
	Brokers string
	// Group is the Kafka consumer group, and the consumer's name for
	// outbox.Handle's records of the events it has handled.
	Group  string
	Prefix string
	Types  []string
	Pool   *pgxpool.Pool
	Handle func(ctx context.Context, tx pgx.Tx, e outbox.Event) error
	Logger *slog.Logger
}

// Consume reads events until ctx is done and handles each with
// outbox.Handle, which skips the ones already handled. A message's offset
// is committed only once it's handled, so a crash leads to redelivery, not
// loss. A handler that fails is retried, holding up its partition rather
// than handling its events out of order.
func Consume(ctx context.Context, cfg ConsumerConfig) (err error) {
	c, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers":  cfg.Brokers,
		"group.id":           cfg.Group,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer func() { err = errors.Join(err, c.Close()) }()
	topics := make([]string, len(cfg.Types))
	for i, t := range cfg.Types {
		topics[i] = Topic(cfg.Prefix, t)
	}
	if err = c.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	for ctx.Err() == nil {
		msg, readErr := c.ReadMessage(200 * time.Millisecond)
		if readErr != nil {
			var kerr ckafka.Error
			switch {
			case errors.As(readErr, &kerr) && kerr.IsTimeout():
			case errors.As(readErr, &kerr) && !kerr.IsFatal():
				cfg.Logger.Warn("kafka consumer", "error", readErr)
			default:
				return fmt.Errorf("read: %w", readErr)
			}
			continue
		}
		var e outbox.Event
		if err = json.Unmarshal(msg.Value, &e); err != nil || e.ID == "" {
			// It can never be handled; holding the partition for it
			// would stop everything behind it.
			cfg.Logger.Error("skipping a message that isn't an event", "partition", msg.TopicPartition, "error", err)
		} else if err = handle(ctx, cfg, e); err != nil {
			return err
		}
		if _, err = c.CommitMessage(msg); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
	return ctx.Err()
}

func handle(ctx context.Context, cfg ConsumerConfig, e outbox.Event) error {
	wait := 100 * time.Millisecond
	for {
		_, err := outbox.Handle(ctx, cfg.Pool, cfg.Group, e, cfg.Handle)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cfg.Logger.Warn("handle event", "event", e.ID, "error", err, "retry_in", wait)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		wait = min(2*wait, 30*time.Second)
	}
}
