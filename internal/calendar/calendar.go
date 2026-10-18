// Package calendar knows which days the US stock market trades, when each
// session closes, and when trades settle.
package calendar

import (
	"context"
	"errors"
	"fmt"
	"time"
	_ "time/tzdata" // sessions are New York times, wherever this runs

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// The market's time zone, and its regular close.
var newYork = mustLoad("America/New_York")

const regularClose = 16 * time.Hour

func mustLoad(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	}
	return loc
}

// Holiday is a weekday the market is closed, or closes early when
// EarlyClose is set (as a time of day in New York, like 13h).
type Holiday struct {
	Day        time.Time // only the date is used
	Name       string
	EarlyClose time.Duration
}

// Querier is the part of pgx that loading a calendar needs.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Calendar is the market's trading days. Days are dates, kept as midnight
// UTC like the ledger's.
type Calendar struct {
	holidays map[time.Time]Holiday
}

// New returns a calendar with the given holidays.
func New(holidays ...Holiday) *Calendar {
	c := &Calendar{holidays: make(map[time.Time]Holiday, len(holidays))}
	for _, h := range holidays {
		h.Day = date(h.Day)
		c.holidays[h.Day] = h
	}
	return c
}

// Load reads the holidays from Postgres.
func Load(ctx context.Context, q Querier) (*Calendar, error) {
	rows, err := q.Query(ctx, `SELECT day, name, closes_at FROM market_holidays`)
	if err != nil {
		return nil, fmt.Errorf("load market holidays: %w", err)
	}
	var (
		holidays []Holiday
		h        Holiday
		closesAt pgtype.Time
	)
	_, err = pgx.ForEachRow(rows, []any{&h.Day, &h.Name, &closesAt}, func() error {
		h.EarlyClose = timeOfDay(closesAt)
		holidays = append(holidays, h)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load market holidays: %w", err)
	}
	return New(holidays...), nil
}

// AddHoliday adds h. Adding the same holiday again changes nothing; a
// different one for the same day is an error.
func AddHoliday(ctx context.Context, tx pgx.Tx, h Holiday) error {
	if h.Day.IsZero() || h.Name == "" {
		return errors.New("a holiday needs a day and a name")
	}
	if wd := date(h.Day).Weekday(); wd == time.Saturday || wd == time.Sunday {
		return fmt.Errorf("%s is a weekend, which is closed anyway", date(h.Day).Format(time.DateOnly))
	}
	var closesAt pgtype.Time
	if h.EarlyClose != 0 {
		if h.EarlyClose <= 9*time.Hour+30*time.Minute || h.EarlyClose >= regularClose || h.EarlyClose%time.Minute != 0 {
			return fmt.Errorf("an early close must be a whole minute between 09:30 and 16:00, got %s", h.EarlyClose)
		}
		closesAt = pgtype.Time{Microseconds: h.EarlyClose.Microseconds(), Valid: true}
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO market_holidays (day, name, closes_at) VALUES ($1, $2, $3)
		ON CONFLICT (day) DO NOTHING`, date(h.Day), h.Name, closesAt)
	if err != nil {
		return fmt.Errorf("add holiday: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var (
		name     string
		existing pgtype.Time
	)
	err = tx.QueryRow(ctx, `SELECT name, closes_at FROM market_holidays WHERE day = $1`, date(h.Day)).
		Scan(&name, &existing)
	if err != nil {
		return fmt.Errorf("add holiday: %w", err)
	}
	if name != h.Name || timeOfDay(existing) != h.EarlyClose {
		return fmt.Errorf("%s already has a different holiday, %q", date(h.Day).Format(time.DateOnly), name)
	}
	return nil
}

// IsTradingDay reports whether the market opens on day.
func (c *Calendar) IsTradingDay(day time.Time) bool {
	day = date(day)
	if wd := day.Weekday(); wd == time.Saturday || wd == time.Sunday {
		return false
	}
	h, ok := c.holidays[day]
	return !ok || h.EarlyClose != 0
}

// NextTradingDay returns the first trading day after day.
func (c *Calendar) NextTradingDay(day time.Time) time.Time {
	day = date(day).AddDate(0, 0, 1)
	for !c.IsTradingDay(day) {
		day = day.AddDate(0, 0, 1)
	}
	return day
}

// Close returns when the market closes on trading day day.
func (c *Calendar) Close(day time.Time) time.Time {
	day = date(day)
	closing := regularClose
	if h, ok := c.holidays[day]; ok && h.EarlyClose != 0 {
		closing = h.EarlyClose
	}
	return time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, newYork).Add(closing)
}

// Session returns the trading day that now belongs to: today in New York
// if it's a trading day and the market hasn't closed yet, otherwise the
// next trading day.
func (c *Calendar) Session(now time.Time) time.Time {
	ny := now.In(newYork)
	today := time.Date(ny.Year(), ny.Month(), ny.Day(), 0, 0, 0, 0, time.UTC)
	if c.IsTradingDay(today) && now.Before(c.Close(today)) {
		return today
	}
	return c.NextTradingDay(today)
}

// SettlementDate returns when a trade made on tradeDate settles: the next
// trading day (T+1).
func (c *Calendar) SettlementDate(tradeDate time.Time) time.Time {
	return c.NextTradingDay(tradeDate)
}

// timeOfDay returns a TIME column as a duration since midnight, and 0 for
// NULL.
func timeOfDay(t pgtype.Time) time.Duration {
	if !t.Valid {
		return 0
	}
	return time.Duration(t.Microseconds) * time.Microsecond
}

func date(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
