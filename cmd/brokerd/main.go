// Command brokerd is the brokerd server and admin tool.
//
// Usage:
//
//	brokerd migrate   apply database migrations
//
// DATABASE_URL must be set.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/AmrMady/brokerd/internal/postgres"
)

const usage = "usage: brokerd migrate"

type config struct {
	DatabaseURL string
}

func loadConfig() (config, error) {
	cfg := config{DatabaseURL: os.Getenv("DATABASE_URL")}
	if cfg.DatabaseURL == "" {
		return config{}, errors.New("DATABASE_URL is required")
	}
	return cfg, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], logger)
	stop()
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, logger *slog.Logger) error {
	if len(args) != 1 {
		return errors.New(usage)
	}
	if args[0] != "migrate" {
		return fmt.Errorf("unknown command %q; %s", args[0], usage)
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err = postgres.Migrate(ctx, pool); err != nil {
		return err
	}
	logger.Info("database migrated")
	return nil
}
