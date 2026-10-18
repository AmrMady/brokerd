package calendar_test

import (
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/AmrMady/brokerd/internal/calendar"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

func TestAddAndLoadHolidays(t *testing.T) {
	t.Parallel()
	pool := pgtest.NewPool(t)
	add := func(h calendar.Holiday) error {
		return pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
			return calendar.AddHoliday(t.Context(), tx, h)
		})
	}
	thanksgiving := calendar.Holiday{Day: day("2026-11-26"), Name: "Thanksgiving Day"}
	early := calendar.Holiday{Day: day("2026-11-27"), Name: "Day after Thanksgiving", EarlyClose: 13 * time.Hour}
	for _, h := range []calendar.Holiday{thanksgiving, early, thanksgiving} {
		if err := add(h); err != nil {
			t.Fatalf("add %+v: %v", h, err)
		}
	}
	for _, h := range []calendar.Holiday{
		{Day: day("2026-11-26"), Name: "Something else"},
		{Day: day("2026-11-27"), Name: "Day after Thanksgiving", EarlyClose: 14 * time.Hour},
		{Day: day("2026-11-28"), Name: "A Saturday"},
		{Day: day("2026-12-24"), Name: "Christmas Eve", EarlyClose: 17 * time.Hour},
		{Day: day("2026-12-24"), Name: "Christmas Eve", EarlyClose: 13*time.Hour + time.Second},
		{Day: day("2026-12-25")},
	} {
		if err := add(h); err == nil {
			t.Errorf("AddHoliday(%+v) succeeded, want an error", h)
		}
	}

	loaded, err := calendar.Load(t.Context(), pool)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.IsTradingDay(day("2026-11-26")) || !loaded.IsTradingDay(day("2026-11-27")) {
		t.Error("loaded calendar has the wrong trading days")
	}
	if got := loaded.Close(day("2026-11-27")); !got.Equal(at("2026-11-27T18:00:00Z")) {
		t.Errorf("loaded early close %s, want 18:00 UTC", got.UTC())
	}
}
