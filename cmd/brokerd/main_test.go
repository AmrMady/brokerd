package main

import (
	"fmt"
	"os"
	"testing"

	"go.uber.org/goleak"

	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

// TestMain stops the test database before looking for leaked goroutines,
// since its client's goroutines only end then.
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	if code == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			code = 1
		}
	}
	os.Exit(code)
}
