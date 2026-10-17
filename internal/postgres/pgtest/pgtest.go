// Package pgtest gives each test a fresh, migrated Postgres database.
//
// It starts a Postgres container on first use, or uses the server in
// BROKERD_TEST_DATABASE_URL if set. Each test database is a copy of a
// migrated template, so tests are isolated and can run in parallel.
package pgtest

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/AmrMady/brokerd/internal/postgres"
)

// EnvURL is the environment variable for using an existing Postgres server.
const EnvURL = "BROKERD_TEST_DATABASE_URL"

type server struct {
	adminURL  *url.URL
	template  string
	container *tcpostgres.PostgresContainer
}

var (
	startOnce sync.Once
	srv       server
	errStart  error

	// Postgres can refuse to copy a template that is being copied.
	createMu sync.Mutex
	dbSeq    atomic.Int64
)

// NewPool returns a pool for a new database that is dropped when the test
// ends. It skips the test in -short mode.
func NewPool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	if testing.Short() {
		tb.Skip("skipping database test in -short mode")
	}
	startOnce.Do(func() { srv, errStart = start(context.Background()) })
	if errStart != nil {
		tb.Fatalf("start test database server: %v", errStart)
	}

	ctx := context.Background()
	name := fmt.Sprintf("brokerd_test_%d_%d", os.Getpid(), dbSeq.Add(1))
	createMu.Lock()
	err := srv.exec(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, srv.template))
	createMu.Unlock()
	if err != nil {
		tb.Fatalf("create test database: %v", err)
	}

	pool, err := postgres.NewPool(ctx, srv.urlFor(name))
	if err != nil {
		tb.Fatalf("connect to test database: %v", err)
	}
	tb.Cleanup(func() {
		pool.Close()
		srv.dropDatabase(tb, name)
	})
	return pool
}

func (s server) dropDatabase(tb testing.TB, name string) {
	if err := s.exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
		tb.Errorf("drop test database: %v", err)
	}
}

// Stop removes the container or the template database. Call it from TestMain
// after m.Run.
func Stop() {
	ctx := context.Background()
	switch {
	case srv.container != nil:
		if err := srv.container.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "pgtest: stop container: %v\n", err)
		}
	case srv.adminURL != nil:
		if err := srv.exec(ctx, "DROP DATABASE IF EXISTS "+srv.template+" WITH (FORCE)"); err != nil {
			fmt.Fprintf(os.Stderr, "pgtest: drop template database: %v\n", err)
		}
	}
}

func start(ctx context.Context) (server, error) {
	// go test runs packages in parallel, so each test binary gets its own template.
	s := server{template: fmt.Sprintf("brokerd_template_%d", os.Getpid())}
	adminURL := os.Getenv(EnvURL)
	if adminURL == "" {
		ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("brokerd"),
			tcpostgres.WithUsername("brokerd"),
			tcpostgres.WithPassword("brokerd"),
			tcpostgres.BasicWaitStrategies(),
			testcontainers.WithCmdArgs("-c", "fsync=off", "-c", "max_connections=500"),
		)
		if err != nil {
			return s, fmt.Errorf("start postgres container: %w", err)
		}
		s.container = ctr
		if adminURL, err = ctr.ConnectionString(ctx, "sslmode=disable"); err != nil {
			return s, fmt.Errorf("container connection string: %w", err)
		}
	}
	u, err := url.Parse(adminURL)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return s, fmt.Errorf("%s must be a postgres:// URL", EnvURL)
	}
	s.adminURL = u

	if err = s.exec(ctx, "DROP DATABASE IF EXISTS "+s.template+" WITH (FORCE)"); err != nil {
		return s, err
	}
	if err = s.exec(ctx, "CREATE DATABASE "+s.template); err != nil {
		return s, err
	}
	pool, err := postgres.NewPool(ctx, s.urlFor(s.template))
	if err != nil {
		return s, err
	}
	defer pool.Close() // a template can't be copied while connections are open
	return s, postgres.Migrate(ctx, pool)
}

func (s server) exec(ctx context.Context, sql string) (err error) {
	conn, err := pgx.Connect(ctx, s.adminURL.String())
	if err != nil {
		return fmt.Errorf("connect to test server: %w", err)
	}
	defer func() { err = errors.Join(err, conn.Close(ctx)) }()
	if _, err = conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("%s: %w", sql, err)
	}
	return nil
}

func (s server) urlFor(name string) string {
	u := *s.adminURL
	u.Path = "/" + name
	q := u.Query()
	q.Set("pool_max_conns", "16")
	u.RawQuery = q.Encode()
	return u.String()
}
