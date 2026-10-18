package calendar_test

import (
	"testing"
	"time"

	"github.com/AmrMady/brokerd/internal/calendar"
)

func day(s string) time.Time {
	d, err := time.Parse(time.DateOnly, s)
	if err != nil {
		panic(err)
	}
	return d
}

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// Thanksgiving 2026 is Thursday November 26, and the market closes at 13:00
// the day after.
var cal = calendar.New(
	calendar.Holiday{Day: day("2026-11-26"), Name: "Thanksgiving Day"},
	calendar.Holiday{Day: day("2026-11-27"), Name: "Day after Thanksgiving", EarlyClose: 13 * time.Hour},
)

func TestTradingDays(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		day  string
		want bool
	}{
		{"2026-11-23", true},  // Monday
		{"2026-11-26", false}, // holiday
		{"2026-11-27", true},  // early close
		{"2026-11-28", false}, // Saturday
		{"2026-11-29", false}, // Sunday
	} {
		if got := cal.IsTradingDay(day(tt.day)); got != tt.want {
			t.Errorf("IsTradingDay(%s) = %t, want %t", tt.day, got, tt.want)
		}
	}
}

func TestSettlementDate(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ trade, want string }{
		{"2026-11-24", "2026-11-25"},
		{"2026-11-25", "2026-11-27"}, // over the holiday
		{"2026-11-27", "2026-11-30"}, // over the weekend
	} {
		if got := cal.SettlementDate(day(tt.trade)); !got.Equal(day(tt.want)) {
			t.Errorf("SettlementDate(%s) = %s, want %s", tt.trade, got.Format(time.DateOnly), tt.want)
		}
	}
}

func TestClose(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ day, want string }{
		{"2026-10-16", "2026-10-16T20:00:00Z"}, // 16:00 EDT
		{"2026-11-25", "2026-11-25T21:00:00Z"}, // 16:00 EST
		{"2026-11-27", "2026-11-27T18:00:00Z"}, // 13:00 EST, early close
	} {
		if got := cal.Close(day(tt.day)); !got.Equal(at(tt.want)) {
			t.Errorf("Close(%s) = %s, want %s", tt.day, got.UTC().Format(time.RFC3339), tt.want)
		}
	}
}

func TestSession(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ now, want string }{
		{"2026-11-24T14:30:00Z", "2026-11-24"}, // during the day
		{"2026-11-24T20:59:59Z", "2026-11-24"}, // a second before the close
		{"2026-11-24T21:00:00Z", "2026-11-25"}, // at the close
		{"2026-11-25T23:00:00Z", "2026-11-27"}, // after the close, before the holiday
		{"2026-11-27T18:30:00Z", "2026-11-30"}, // after the early close, Friday
		{"2026-11-28T15:00:00Z", "2026-11-30"}, // Saturday
		{"2026-11-25T03:00:00Z", "2026-11-25"}, // 22:00 on the 24th in New York, after its close
	} {
		if got := cal.Session(at(tt.now)); !got.Equal(day(tt.want)) {
			t.Errorf("Session(%s) = %s, want %s", tt.now, got.Format(time.DateOnly), tt.want)
		}
	}
}

func TestIsOpen(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		now  string
		want bool
	}{
		{"2026-10-16T13:29:00Z", false}, // Friday, 09:29 EDT
		{"2026-10-16T13:30:00Z", true},  // the open
		{"2026-10-16T19:59:59Z", true},  // a second before the close
		{"2026-10-16T20:00:00Z", false}, // 16:00 EDT, the close
		{"2026-10-17T15:00:00Z", false}, // Saturday
		{"2026-10-18T15:00:00Z", false}, // Sunday
		{"2026-11-02T13:30:00Z", false}, // the clocks went back: 08:30 EST
		{"2026-11-02T14:30:00Z", true},  // 09:30 EST
		{"2026-11-02T20:59:59Z", true},
		{"2026-11-02T21:00:00Z", false},
		{"2026-03-09T13:30:00Z", true},  // the clocks went forward: 09:30 EDT
		{"2026-11-26T16:00:00Z", false}, // Thanksgiving
		{"2026-11-27T17:59:59Z", true},  // a second before the early close
		{"2026-11-27T18:00:00Z", false}, // 13:00 EST, the early close
	} {
		if got := cal.IsOpen(at(tt.now)); got != tt.want {
			t.Errorf("IsOpen(%s) = %t, want %t", tt.now, got, tt.want)
		}
	}
}
