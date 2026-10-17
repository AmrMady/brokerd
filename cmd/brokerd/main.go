// Command brokerd is the brokerd server and admin tool.
//
// Usage:
//
//	brokerd migrate     apply database migrations
//	brokerd reconcile   check the ledger, exit 1 on any mismatch
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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/postgres"
)

const usage = "usage: brokerd migrate | reconcile"

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
	var command func(context.Context, *pgxpool.Pool, *slog.Logger) error
	switch args[0] {
	case "migrate":
		command = migrate
	case "reconcile":
		command = reconcile
	default:
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
	return command(ctx, pool, logger)
}

func migrate(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	if err := postgres.Migrate(ctx, pool); err != nil {
		return err
	}
	logger.Info("database migrated")
	return nil
}

func reconcile(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	report, err := ledger.Reconcile(ctx, pool)
	if err != nil {
		return err
	}
	for _, m := range report.Mismatches {
		logger.Error("ledger mismatch",
			"check", m.Check, "account", m.AccountID, "asset", m.Asset, "want", m.Want, "got", m.Got)
	}
	if !report.OK() {
		return fmt.Errorf("reconcile found %d mismatches", len(report.Mismatches))
	}
	logger.Info("ledger reconciled", "ledger_accounts", report.LedgerAccounts, "lines", report.Lines)
	return nil
}
