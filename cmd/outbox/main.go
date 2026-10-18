// Command outbox publishes brokerd's outbox to Kafka, and consumes it. It's
// separate from brokerd because its Kafka client needs cgo.
//
// Usage:
//
//	outbox publish [flags]  publish the outbox; of several, one at a time leads
//	outbox consume [flags]  log each event once, as consumer group -group
//
// DATABASE_URL must be set.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/AmrMady/brokerd/internal/outbox"
	"github.com/AmrMady/brokerd/internal/outbox/kafka"
	"github.com/AmrMady/brokerd/internal/postgres"
)

const usage = "usage: outbox publish | consume [flags]"

var types = []string{outbox.EntryPosted, outbox.OrderFilled}

type config struct {
	command     string
	databaseURL string
	brokers     string
	prefix      string
	partitions  int
	replication int
	group       string
}

func parse(args []string, output io.Writer) (config, error) {
	if len(args) == 0 || (args[0] != "publish" && args[0] != "consume") {
		return config{}, errors.New(usage)
	}
	cfg := config{command: args[0], databaseURL: os.Getenv("DATABASE_URL")}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(output)
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	fs.StringVar(&cfg.brokers, "brokers", brokers, "Kafka brokers, comma-separated (default $KAFKA_BROKERS)")
	fs.StringVar(&cfg.prefix, "prefix", "brokerd.", "topic prefix; topics are the prefix and the event type")
	fs.IntVar(&cfg.partitions, "partitions", 6, "publish: partitions of new topics")
	fs.IntVar(&cfg.replication, "replication", 1, "publish: replication factor of new topics")
	fs.StringVar(&cfg.group, "group", "audit", "consume: consumer group")
	if err := fs.Parse(args[1:]); err != nil {
		return cfg, err
	}
	if cfg.databaseURL == "" {
		return cfg, errors.New("DATABASE_URL is required")
	}
	return cfg, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], logger)
	stop()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, flag.ErrHelp) {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, logger *slog.Logger) error {
	cfg, err := parse(args, os.Stderr)
	if err != nil {
		return err
	}
	if cfg.command == "publish" {
		return publish(ctx, cfg, logger)
	}
	return consume(ctx, cfg, logger)
}

func publish(ctx context.Context, cfg config, logger *slog.Logger) error {
	pub, err := kafka.NewPublisher(cfg.brokers, cfg.prefix, logger)
	if err != nil {
		return err
	}
	defer pub.Close()
	setup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err = pub.EnsureTopics(setup, types, cfg.partitions, cfg.replication); err != nil {
		return err
	}
	relay, err := outbox.NewRelay(outbox.RelayConfig{DatabaseURL: cfg.databaseURL, Publisher: pub, Logger: logger})
	if err != nil {
		return err
	}
	logger.Info("publishing the outbox", "brokers", cfg.brokers)
	err = relay.Run(ctx)
	logger.Info("stopped publishing", "published", relay.Published())
	return err
}

// consume logs each event once. Records of handled events are kept for a
// week, longer than Kafka keeps the events by default.
func consume(ctx context.Context, cfg config, logger *slog.Logger) error {
	pool, err := postgres.NewPool(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, forgetErr := outbox.ForgetConsumed(ctx, pool, time.Now().Add(-7*24*time.Hour)); forgetErr != nil {
					logger.Error("forget consumed events", "error", forgetErr)
				}
			}
		}
	}()
	logger.Info("consuming", "brokers", cfg.brokers, "group", cfg.group)
	return kafka.Consume(ctx, kafka.ConsumerConfig{
		Brokers: cfg.brokers, Group: cfg.group, Prefix: cfg.prefix, Types: types, Pool: pool, Logger: logger,
		Handle: func(_ context.Context, _ pgx.Tx, e outbox.Event) error {
			logger.Info("event", "id", e.ID, "type", e.Type, "key", e.Key, "payload", e.Payload)
			return nil
		},
	})
}
