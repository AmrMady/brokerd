package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/stream"
)

const shutdownTimeout = 10 * time.Second

type serveConfig struct {
	addr     string
	feed     string
	symbols  []string
	rate     int
	seed     uint64
	file     string
	speed    float64
	upstream string
	grace    time.Duration
}

func parseServe(args []string, output io.Writer) (serveConfig, error) {
	var (
		cfg     serveConfig
		symbols string
	)
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&cfg.addr, "addr", ":8080", "address to listen on")
	fs.StringVar(&cfg.feed, "feed", "sim", "where events come from: sim, replay or relay")
	fs.StringVar(&symbols, "symbols", "AAPL,MSFT,NVDA,AMZN,GOOG", "comma-separated symbols, for sim and relay")
	fs.IntVar(&cfg.rate, "rate", 1000, "sim: events per second")
	fs.Uint64Var(&cfg.seed, "seed", 1, "sim: random seed")
	fs.StringVar(&cfg.file, "file", "", "replay: CSV file of events")
	fs.Float64Var(&cfg.speed, "speed", 1, "replay: 1 is real time, 10 is ten times faster, 0 is as fast as possible")
	fs.StringVar(&cfg.upstream, "upstream", "", "relay: another brokerd's stream, like ws://core:8080/v1/stream")
	fs.DurationVar(&cfg.grace, "grace", time.Second, "how long bars wait for late trades")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("serve takes no arguments, got %q", fs.Args())
	}
	cfg.symbols = strings.Split(symbols, ",")
	switch {
	case cfg.feed == "replay" && cfg.file == "":
		return cfg, errors.New("replay needs -file")
	case cfg.feed == "relay" && cfg.upstream == "":
		return cfg, errors.New("relay needs -upstream")
	case cfg.feed != "sim" && cfg.feed != "replay" && cfg.feed != "relay":
		return cfg, fmt.Errorf("feed must be sim, replay or relay, got %q", cfg.feed)
	}
	return cfg, nil
}

// newFeed returns the configured feed and a function that releases it.
func (cfg serveConfig) newFeed(logger *slog.Logger) (marketdata.Feed, func() error, error) {
	noop := func() error { return nil }
	switch cfg.feed {
	case "replay":
		f, err := os.Open(cfg.file)
		if err != nil {
			return nil, nil, err
		}
		replay, err := marketdata.NewReplay(f, cfg.speed)
		if err != nil {
			return nil, nil, errors.Join(err, f.Close())
		}
		return replay, f.Close, nil
	case "relay":
		relay, err := stream.NewRelay(stream.RelayConfig{URL: cfg.upstream, Symbols: cfg.symbols, Logger: logger})
		return relay, noop, err
	default:
		sim, err := marketdata.NewSimulator(marketdata.SimulatorConfig{
			Symbols: cfg.symbols, Seed: cfg.seed, Rate: cfg.rate, Paced: true,
		})
		return sim, noop, err
	}
}

// serve runs the market data pipeline and streams it from ln until ctx is
// done or the feed ends:
//
//	feed -> sequencer -> 1s and 1m bars -> hub
//	                  -> hub
func serve(ctx context.Context, cfg serveConfig, ln net.Listener, logger *slog.Logger) (err error) {
	feed, release, err := cfg.newFeed(logger)
	if err != nil {
		return errors.Join(err, ln.Close())
	}
	defer func() { err = errors.Join(err, release()) }()

	hub, err := stream.NewHub(stream.Config{Logger: logger})
	if err != nil {
		return errors.Join(err, ln.Close())
	}
	seconds, err := marketdata.NewAggregator(time.Second, cfg.grace, hub)
	if err != nil {
		return errors.Join(err, ln.Close())
	}
	minutes, err := marketdata.NewAggregator(time.Minute, cfg.grace, hub)
	if err != nil {
		return errors.Join(err, ln.Close())
	}
	sequencer := marketdata.NewSequencer(marketdata.Handlers{seconds, minutes, hub})

	mux := http.NewServeMux()
	mux.Handle("GET /v1/stream", hub)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	feedCtx, stopFeed := context.WithCancel(ctx)
	defer stopFeed()
	fed := make(chan error, 1)
	go func() { fed <- feed.Run(feedCtx, sequencer) }()
	logger.Info("serving market data", "addr", ln.Addr().String(), "feed", cfg.feed, "symbols", cfg.symbols)

	// The aggregators belong to the feed's goroutine until it has
	// returned, so they're only flushed after that.
	select {
	case err = <-fed:
		if err == nil {
			seconds.Flush()
			minutes.Flush()
			logger.Info("feed ended")
		}
	case err = <-served:
		stopFeed()
		<-fed
	case <-ctx.Done():
		stopFeed()
		if err = <-fed; errors.Is(err, context.Canceled) {
			err = nil
		}
	}

	// The HTTP server doesn't wait for WebSocket connections, which the
	// hub closes and waits for.
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err = errors.Join(err, srv.Shutdown(sctx), hub.Close(sctx))

	seq, s, m, h := sequencer.Stats(), seconds.Stats(), minutes.Stats(), hub.Stats()
	logger.Info("market data stopped",
		"gaps", seq.Gaps, "missed", seq.Missed, "stale", seq.Stale,
		"bars", s.Bars+m.Bars, "late_1s", s.Late, "late_1m", m.Late,
		"dropped", h.Dropped, "disconnected", h.Disconnected)
	return err
}
