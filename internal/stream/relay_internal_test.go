package stream

import (
	"slices"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	t.Parallel()
	b := backoff{first: 10 * time.Second}
	var got []time.Duration
	for _, streamed := range []bool{false, false, false, false, false, true, false} {
		got = append(got, b.after(streamed))
	}
	s := time.Second
	want := []time.Duration{10 * s, 20 * s, 40 * s, time.Minute, time.Minute, 10 * s, 20 * s}
	if !slices.Equal(got, want) {
		t.Errorf("waits %v, want %v", got, want)
	}
}
