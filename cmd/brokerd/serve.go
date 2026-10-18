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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AmrMady/brokerd/internal/api"
	"github.com/AmrMady/brokerd/internal/calendar"
	"github.com/AmrMady/brokerd/internal/engine"
	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/metrics"
	"github.com/AmrMady/brokerd/internal/postgres"
	"github.com/AmrMady/brokerd/internal/stream"
)

const (
	shutdownTimeout = 10 * time.Second
	barsKept        = 1000 // of each symbol and timeframe, for the API
)

type serveConfig struct {
	databaseURL string // optional; the engine and API need it
	addr        string
	feed        string
	symbols     []string
	rate        int
	seed        uint64
	file        string
	speed       float64
	upstream    string
	grace       time.Duration
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

// serve runs the market data pipeline and serves it from ln until ctx is
// done or the feed ends:
//
//	feed -> sequencer -> quote cache -> engine
//	                  -> 1s and 1m bars -> hub
//	                  -> hub
//
// The engine and the API only run with a database; without one, serve only
// streams market data, which is all a relay needs.
func serve(ctx context.Context, cfg serveConfig, ln net.Listener, logger *slog.Logger) (err error) {
	defer func() {
		if closeErr := ln.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}()
	feed, release, err := cfg.newFeed(logger)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, release()) }()

	hub, err := stream.NewHub(stream.Config{Logger: logger})
	if err != nil {
		return err
	}
	bars := marketdata.NewBarStore(barsKept)
	seconds, err := marketdata.NewAggregator(time.Second, cfg.grace, marketdata.BarHandlers{hub, bars})
	if err != nil {
		return err
	}
	minutes, err := marketdata.NewAggregator(time.Minute, cfg.grace, marketdata.BarHandlers{hub, bars})
	if err != nil {
		return err
	}
	quotes := &marketdata.QuoteCache{}
	handlers := marketdata.Handlers{quotes}
	reg := metrics.New()
	reg.Stream(hub)

	apiCfg := api.Config{
		Quotes: quotes, Bars: bars, Logger: logger,
		ObserveRequest: reg.ObserveRequest, ObservePost: reg.ObservePost,
	}
	var eng *engine.Engine
	if cfg.databaseURL != "" {
		var pool *pgxpool.Pool
		if pool, err = postgres.NewPool(ctx, cfg.databaseURL); err != nil {
			return err
		}
		defer pool.Close()
		if eng, err = startEngine(ctx, cfg, pool, quotes, logger, reg); err != nil {
			return err
		}
		if err = api.MaintainIdempotency(ctx, pool, time.Now()); err != nil {
			return err
		}
		maintainCtx, stopMaintaining := context.WithCancel(context.Background())
		defer stopMaintaining()
		go maintain(maintainCtx, pool, logger)
		handlers = append(handlers, eng) // after the cache, so it reads the newest quote
		apiCfg.Pool, apiCfg.Engine = pool, eng
	}
	apiSrv := api.New(apiCfg)
	apiSrv.Handle("GET /v1/stream", hub)
	apiSrv.Handle("GET /metrics", reg.Handler())
	sequencer := marketdata.NewSequencer(append(handlers, seconds, minutes, hub))
	reg.MarketData(sequencer, map[string]*marketdata.Aggregator{"1s": seconds, "1m": minutes})

	srv := &http.Server{
		Handler:           apiSrv,
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	engineCtx, stopEngine := context.WithCancel(context.Background())
	defer stopEngine()
	engineDone := make(chan error, 1)
	if eng != nil {
		go func() { engineDone <- eng.Run(engineCtx) }()
	} else {
		close(engineDone)
	}

	feedCtx, stopFeed := context.WithCancel(ctx)
	defer stopFeed()
	fed := make(chan error, 1)
	go func() { fed <- feed.Run(feedCtx, sequencer) }()
	logger.Info("serving", "addr", ln.Addr().String(), "feed", cfg.feed, "symbols", cfg.symbols, "api", eng != nil)

	feedEnded := false
	select {
	case err = <-fed:
		feedEnded = true
		if err == nil {
			// The aggregators belong to the feed's goroutine until it has
			// returned, so they're only flushed now.
			seconds.Flush()
			minutes.Flush()
			logger.Info("feed ended")
		}
	case err = <-served:
	case <-ctx.Done():
	}

	// Stop taking requests and finish the ones in flight while quotes still
	// flow, then stop the feed and the engine, then the WebSocket clients,
	// which the HTTP server doesn't wait for. The pool closes last.
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	apiSrv.Drain()
	err = errors.Join(err, srv.Shutdown(sctx))
	stopFeed()
	if !feedEnded {
		if feedErr := <-fed; !errors.Is(feedErr, context.Canceled) {
			err = errors.Join(err, feedErr)
		}
	}
	stopEngine()
	if engineErr, ok := <-engineDone; ok && !errors.Is(engineErr, context.Canceled) {
		err = errors.Join(err, engineErr)
	}
	err = errors.Join(err, hub.Close(sctx))

	seq, s, m, h := sequencer.Stats(), seconds.Stats(), minutes.Stats(), hub.Stats()
	logger.Info("stopped",
		"gaps", seq.Gaps, "missed", seq.Missed, "stale", seq.Stale,
		"bars", s.Bars+m.Bars, "late_1s", s.Late, "late_1m", m.Late,
		"dropped", h.Dropped, "disconnected", h.Disconnected)
	return err
}

// maintain keeps the idempotency partitions current, hourly, until ctx is
// done.
func maintain(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := api.MaintainIdempotency(ctx, pool, now); err != nil && ctx.Err() == nil {
				logger.Error("maintain idempotency keys", "error", err)
			}
		}
	}
}

// startEngine makes sure the feed's symbols can be traded and starts the
// engine with the market calendar.
func startEngine(ctx context.Context, cfg serveConfig, pool *pgxpool.Pool, quotes engine.Quotes, logger *slog.Logger,
	reg *metrics.Registry,
) (*engine.Engine, error) {
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		for _, symbol := range cfg.symbols {
			if _, err := ledger.AssetScale(ctx, tx, ledger.Asset(symbol)); err == nil {
				continue
			}
			if err := ledger.AddAsset(ctx, tx, ledger.Asset(symbol), 9); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("add symbols: %w", err)
	}
	cal, err := calendar.Load(ctx, pool)
	if err != nil {
		return nil, err
	}
	return engine.New(ctx, engine.Config{
		Pool: pool, Quotes: quotes, Calendar: cal, Logger: logger,
		ObserveFill: reg.ObserveFill, ObservePost: reg.ObservePost,
	})
}
