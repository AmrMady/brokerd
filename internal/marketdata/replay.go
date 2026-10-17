package marketdata

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

var replayHeader = []string{"type", "symbol", "seq", "timestamp", "price", "size", "bid", "bid_size", "ask", "ask_size"}

// Replay is a Feed of events read from CSV with the header
//
//	type,symbol,seq,timestamp,price,size,bid,bid_size,ask,ask_size
//
// type is trade or quote. Trades fill price and size and quotes fill the
// bid and ask columns; the other columns stay empty. Timestamps are RFC 3339.
// A Replay reads its input once, so it can only run once.
type Replay struct {
	r     io.Reader
	speed float64
}

// NewReplay returns a Replay of the events in r. With speed 0 it sends them
// as fast as it can. Otherwise it keeps the time between them, divided by
// speed: 1 is real time, 10 is ten times faster.
func NewReplay(r io.Reader, speed float64) (*Replay, error) {
	if !(speed >= 0) { // also rejects NaN
		return nil, fmt.Errorf("replay speed must not be negative, got %v", speed)
	}
	return &Replay{r: r, speed: speed}, nil
}

// Run implements Feed.
func (r *Replay) Run(ctx context.Context, h Handler) error {
	cr := csv.NewReader(r.r)
	cr.FieldsPerRecord = len(replayHeader)
	cr.ReuseRecord = true
	header, err := cr.Read()
	if err != nil {
		return fmt.Errorf("read replay header: %w", err)
	}
	if !slices.Equal(header, replayHeader) {
		return fmt.Errorf("replay header is %q, want %q", header, replayHeader)
	}

	var first, wallStart time.Time
	for {
		rec, readErr := cr.Read()
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read replay: %w", readErr)
		}
		line, _ := cr.FieldPos(0)

		var (
			t  Trade
			q  Quote
			ts time.Time
		)
		switch rec[0] {
		case "trade":
			t, err = parseTrade(rec)
			ts = t.Timestamp
		case "quote":
			q, err = parseQuote(rec)
			ts = q.Timestamp
		default:
			err = fmt.Errorf("type must be trade or quote, got %q", rec[0])
		}
		if err != nil {
			return fmt.Errorf("replay line %d: %w", line, err)
		}

		if r.speed > 0 {
			if first.IsZero() {
				first, wallStart = ts, time.Now()
			}
			wait := time.Duration(float64(ts.Sub(first)) / r.speed)
			if err = sleepUntil(ctx, wallStart.Add(wait)); err != nil {
				return err
			}
		} else if err = ctx.Err(); err != nil {
			return err
		}

		if rec[0] == "trade" {
			h.HandleTrade(t)
		} else {
			h.HandleQuote(q)
		}
	}
}

func parseTrade(rec []string) (Trade, error) {
	t := Trade{Symbol: rec[1]}
	err := errors.Join(
		parseCommon(rec, &t.Seq, &t.Timestamp),
		parsePositive("price", rec[4], &t.Price),
		parsePositive("size", rec[5], &t.Size),
		requireEmpty(rec, 6, 7, 8, 9),
	)
	return t, err
}

func parseQuote(rec []string) (Quote, error) {
	q := Quote{Symbol: rec[1]}
	err := errors.Join(
		parseCommon(rec, &q.Seq, &q.Timestamp),
		requireEmpty(rec, 4, 5),
		parsePositive("bid", rec[6], &q.Bid),
		parseSize("bid_size", rec[7], &q.BidSize),
		parsePositive("ask", rec[8], &q.Ask),
		parseSize("ask_size", rec[9], &q.AskSize),
	)
	return q, err
}

func parseCommon(rec []string, seq *uint64, ts *time.Time) error {
	if rec[1] == "" {
		return errors.New("symbol is empty")
	}
	var err error
	if *seq, err = strconv.ParseUint(rec[2], 10, 64); err != nil {
		return fmt.Errorf("seq: %w", err)
	}
	if *ts, err = time.Parse(time.RFC3339Nano, rec[3]); err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}
	return nil
}

func parsePositive(name, s string, d *decimal.Decimal) error {
	if err := parseSize(name, s, d); err != nil {
		return err
	}
	if d.IsZero() {
		return fmt.Errorf("%s must be positive, got %s", name, s)
	}
	return nil
}

func parseSize(name, s string, d *decimal.Decimal) error {
	v, err := decimal.NewFromString(s)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if v.IsNegative() {
		return fmt.Errorf("%s must not be negative, got %s", name, s)
	}
	*d = v
	return nil
}

func requireEmpty(rec []string, columns ...int) error {
	for _, c := range columns {
		if rec[c] != "" {
			return fmt.Errorf("%s must be empty for a %s", replayHeader[c], rec[0])
		}
	}
	return nil
}

func sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
