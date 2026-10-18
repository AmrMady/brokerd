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
	"strconv"
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
	maxConns  int // the server's max_connections
}

var (
	startOnce sync.Once
	srv       server
	errStart  error

	// Postgres can refuse to copy a template that is being copied.
	createMu sync.Mutex
	dbSeq    atomic.Int64
)

// NewURL returns the URL of a new database that is dropped when the test
// ends. Pools made from it have up to 8 connections. It skips the test in
// -short mode.
func NewURL(tb testing.TB) string {
	tb.Helper()
	return newURL(tb, 8)
}

// NewPoolSize is NewPool with up to conns connections, for tests of what
// many transactions do at once, but no more than a quarter of what the
// server allows, so other tests running meanwhile still get theirs.
func NewPoolSize(tb testing.TB, conns int) *pgxpool.Pool {
	tb.Helper()
	pool, err := postgres.NewPool(context.Background(), newURL(tb, conns))
	if err != nil {
		tb.Fatalf("connect to test database: %v", err)
	}
	tb.Cleanup(pool.Close)
	return pool
}

func newURL(tb testing.TB, conns int) string {
	tb.Helper()
	if testing.Short() {
		tb.Skip("skipping database test in -short mode")
	}
	startOnce.Do(func() { srv, errStart = start(context.Background()) })
	if errStart != nil {
		tb.Fatalf("start test database server: %v", errStart)
	}

	name := fmt.Sprintf("brokerd_test_%d_%d", os.Getpid(), dbSeq.Add(1))
	createMu.Lock()
	err := srv.exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, srv.template))
	createMu.Unlock()
	if err != nil {
		tb.Fatalf("create test database: %v", err)
	}
	tb.Cleanup(func() { srv.dropDatabase(tb, name) })
	return srv.urlFor(name, min(conns, srv.maxConns/4))
}

// NewPool returns a pool for a new database that is dropped when the test
// ends. It skips the test in -short mode.
func NewPool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	pool, err := postgres.NewPool(context.Background(), NewURL(tb))
	if err != nil {
		tb.Fatalf("connect to test database: %v", err)
	}
	tb.Cleanup(pool.Close)
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

	if s.maxConns, err = s.maxConnections(ctx); err != nil {
		return s, err
	}
	if err = s.exec(ctx, "DROP DATABASE IF EXISTS "+s.template+" WITH (FORCE)"); err != nil {
		return s, err
	}
	if err = s.exec(ctx, "CREATE DATABASE "+s.template); err != nil {
		return s, err
	}
	pool, err := postgres.NewPool(ctx, s.urlFor(s.template, 8))
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

func (s server) maxConnections(ctx context.Context) (n int, err error) {
	conn, err := pgx.Connect(ctx, s.adminURL.String())
	if err != nil {
		return 0, fmt.Errorf("connect to test server: %w", err)
	}
	defer func() { err = errors.Join(err, conn.Close(ctx)) }()
	err = conn.QueryRow(ctx, `SELECT current_setting('max_connections')::int`).Scan(&n)
	return n, err
}

// urlFor returns the URL of database name, for pools of up to conns
// connections. Every package's tests can share one server when it's given
// in EnvURL, so pools are small unless a test asks otherwise.
func (s server) urlFor(name string, conns int) string {
	u := *s.adminURL
	u.Path = "/" + name
	q := u.Query()
	q.Set("pool_max_conns", strconv.Itoa(max(conns, 1)))
	u.RawQuery = q.Encode()
	return u.String()
}
