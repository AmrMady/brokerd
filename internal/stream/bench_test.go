package stream_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/stream"
)

// BenchmarkHubFanOut publishes trades for 100 symbols to 1,000 clients, each
// subscribed to 10 symbols, so each trade goes to 100 clients. Fast clients
// read as fast as they can; slow ones read one message a millisecond, far
// less than they're sent, so their queues stay full and keep dropping.
// ns/op is the cost of publishing one trade.
func BenchmarkHubFanOut(b *testing.B) {
	for _, slow := range []int{0, 50} {
		b.Run(fmt.Sprintf("slow=%d", slow), func(b *testing.B) {
			benchmarkFanOut(b, 1000, slow)
		})
	}
}

func benchmarkFanOut(b *testing.B, clients, slow int) {
	h, err := stream.NewHub(stream.Config{Buffer: 1024, MaxLag: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	const symbols = 100
	trades := make([]marketdata.Trade, symbols)
	for i := range trades {
		trades[i] = trade(fmt.Sprintf("S%03d", i), 1)
	}

	var wg sync.WaitGroup
	for i := range clients {
		c, connErr := h.Connect()
		if connErr != nil {
			b.Fatal(connErr)
		}
		var s stream.Subscriptions
		for k := range 10 {
			s.Trades = append(s.Trades, trades[(i+k*symbols/10)%symbols].Symbol)
		}
		if err = c.Subscribe(s); err != nil {
			b.Fatal(err)
		}
		limit, pause := 256, time.Duration(0)
		if i < slow {
			limit, pause = 1, time.Millisecond
		}
		wg.Go(func() {
			defer c.Close()
			var msgs [][]byte
			for {
				var nextErr error
				if msgs, nextErr = c.Next(context.Background(), msgs[:0], limit); nextErr != nil {
					return
				}
				time.Sleep(pause)
			}
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		h.HandleTrade(trades[i%symbols])
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)*float64(clients/10)/b.Elapsed().Seconds(), "msgs/s")
	b.ReportMetric(float64(h.Stats().Dropped)/float64(b.N), "drops/op")

	if err = h.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	wg.Wait()
}
