// Command brokerd is the brokerd server and admin tool.
//
// Usage:
//
//	brokerd migrate             apply database migrations
//	brokerd reconcile           check the ledger, exit 1 on any mismatch
//	brokerd settle [YYYY-MM-DD] settle trades due on or before the date
//	                            (default: today in New York)
//	brokerd fee-rate sec YYYY-MM-DD RATE
//	brokerd fee-rate taf YYYY-MM-DD RATE MAX
//	                            add a regulatory fee rate effective on the
//	                            date: per dollar sold for the SEC fee, per
//	                            share sold for TAF, with TAF's maximum per trade
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
	"strings"
	"syscall"
	"time"
	_ "time/tzdata" // settlement dates are New York dates, wherever this runs

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/postgres"
)

const usage = "usage: brokerd migrate | reconcile | settle [YYYY-MM-DD] | fee-rate sec|taf YYYY-MM-DD RATE [MAX]"

const settleBatchSize = 500

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
	if len(args) == 0 {
		return errors.New(usage)
	}
	var command func(context.Context, *pgxpool.Pool, *slog.Logger) error
	switch {
	case args[0] == "migrate" && len(args) == 1:
		command = migrate
	case args[0] == "reconcile" && len(args) == 1:
		command = reconcile
	case args[0] == "settle" && len(args) <= 2:
		asOf, err := settlementDate(args[1:])
		if err != nil {
			return err
		}
		command = func(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
			return settle(ctx, pool, logger, asOf)
		}
	case args[0] == "fee-rate" && (len(args) == 4 || len(args) == 5):
		rate, err := parseFeeRate(args[1:])
		if err != nil {
			return err
		}
		command = func(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
			return addFeeRate(ctx, pool, logger, rate)
		}
	default:
		return fmt.Errorf("unknown command %q; %s", strings.Join(args, " "), usage)
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

func settlementDate(args []string) (time.Time, error) {
	if len(args) == 1 {
		d, err := time.Parse(time.DateOnly, args[0])
		if err != nil {
			return time.Time{}, fmt.Errorf("settlement date: %w", err)
		}
		return d, nil
	}
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		return time.Time{}, err
	}
	return time.Now().In(newYork), nil
}

// settle runs SettleDue in batches, each in its own transaction, until
// nothing due is left.
func settle(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, asOf time.Time) error {
	total := 0
	for {
		n, err := settleBatch(ctx, pool, asOf)
		if err != nil {
			return err
		}
		total += n
		if n < settleBatchSize {
			break
		}
	}
	logger.Info("trades settled", "as_of", asOf.Format(time.DateOnly), "settled", total)
	return nil
}

func settleBatch(ctx context.Context, pool *pgxpool.Pool, asOf time.Time) (int, error) {
	var n int
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) (txErr error) {
		n, txErr = ledger.SettleDue(ctx, tx, asOf, settleBatchSize)
		return txErr
	}); err != nil {
		return 0, err
	}
	return n, nil
}

func parseFeeRate(args []string) (ledger.FeeRate, error) {
	r := ledger.FeeRate{Fee: ledger.RegulatoryFee(args[0])}
	var err error
	if r.EffectiveOn, err = time.Parse(time.DateOnly, args[1]); err != nil {
		return r, fmt.Errorf("effective date: %w", err)
	}
	if r.Rate, err = decimal.NewFromString(args[2]); err != nil {
		return r, fmt.Errorf("rate: %w", err)
	}
	if len(args) == 4 {
		var maxPerTrade decimal.Decimal
		if maxPerTrade, err = decimal.NewFromString(args[3]); err != nil {
			return r, fmt.Errorf("maximum per trade: %w", err)
		}
		r.MaxPerTrade = decimal.NewNullDecimal(maxPerTrade)
	}
	return r, nil
}

func addFeeRate(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, r ledger.FeeRate) error {
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return ledger.AddFeeRate(ctx, tx, r)
	}); err != nil {
		return err
	}
	logger.Info("fee rate added", "fee", r.Fee, "effective_on", r.EffectiveOn.Format(time.DateOnly),
		"rate", r.Rate, "max_per_trade", r.MaxPerTrade)
	return nil
}
