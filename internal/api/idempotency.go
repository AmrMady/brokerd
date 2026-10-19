package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// keyRetention is how long a response is kept for retries.
	keyRetention = 24 * time.Hour
	// claimLease is how long a request can run before its key is taken
	// to be abandoned, by a process that crashed, and can be claimed again.
	// Handlers get at most half of it (see handlerTimeout), so one that's
	// only slow stops, and gives its key back, before the lease is up.
	claimLease = time.Minute
)

// idempotent makes a mutating handler safe to retry when the request has an
// Idempotency-Key header. The first request with a key claims it, runs, and
// stores its response; a retry gets the stored response back, with an
// Idempotent-Replayed header. A retry while the first is still running gets
// 409, and the same key with a different request gets 422. A 5xx response,
// or a request whose client went away, isn't stored, so the request can be
// retried.
//
// Keys are scoped to the account in the path, so h must be on a route
// under one, behind authorized.
//
// Claiming is a short transaction under an advisory lock on the key, so it
// works across the table's day partitions, and no connection is held while
// the handler runs.
func (s *Server) idempotent(h handlerFunc) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			return h(w, r)
		}
		if len(key) > 255 || strings.ContainsFunc(key, func(c rune) bool { return c < ' ' || c == 0x7f }) {
			return newError(http.StatusUnprocessableEntity, "invalid_request", "Idempotency-Key must be at most 255 printable characters")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				return newError(http.StatusRequestEntityTooLarge, "too_large", "request body is over %d bytes", tooBig.Limit)
			}
			return err
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		fp := sha256.Sum256([]byte(r.Method + " " + r.URL.Path + "\n" + string(body)))
		scope := r.PathValue("account")

		claimedAt, stored, err := s.claim(r.Context(), scope, key, fp[:])
		if err != nil {
			return err
		}
		if stored != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(stored.status)
			_, _ = w.Write(stored.body) //nolint:errcheck // the client went away; nothing to do
			return nil
		}

		rec := &bufferedResponse{header: http.Header{}, status: http.StatusOK}
		ctx, cancel := context.WithTimeout(r.Context(), s.handlerTimeout)
		err = h(rec, r.WithContext(ctx))
		cancel()
		if err != nil {
			s.writeError(rec, r, err)
		}
		// The request has had its effect by now, so it's answered even if
		// storing the response fails.
		if err = s.finish(context.WithoutCancel(r.Context()), scope, key, claimedAt, rec); err != nil {
			s.cfg.Logger.Error("store idempotent response", "request_id", requestID(r.Context()), "error", err)
		}
		for k, v := range rec.header {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body.Bytes()) //nolint:errcheck // the client went away; nothing to do
		return nil
	}
}

type storedResponse struct {
	status int
	body   []byte
}

// claim takes the key for this request, or returns the stored response of
// an earlier one.
func (s *Server) claim(ctx context.Context, scope, key string, fp []byte) (time.Time, *storedResponse, error) {
	var (
		claimedAt time.Time
		stored    *storedResponse
	)
	err := pgx.BeginFunc(ctx, s.cfg.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '1s'`); err != nil {
			return err
		}
		// Keys have no control characters, so a newline can't be part of one.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, scope+"\n"+key); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "55P03" { // lock_not_available
				return errInProgress
			}
			return err
		}
		var (
			prevFP    []byte
			status    *int16
			body      []byte
			createdAt time.Time
			running   bool
		)
		err := tx.QueryRow(ctx, `
			SELECT fingerprint, status, body, created_at, status IS NULL AND updated_at > now() - $3::interval
			FROM idempotent_requests
			WHERE scope = $1 AND key = $2 AND created_at > now() - $4::interval
			ORDER BY created_at DESC
			LIMIT 1`, scope, key, claimLease, keyRetention).Scan(&prevFP, &status, &body, &createdAt, &running)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return tx.QueryRow(ctx, `
				INSERT INTO idempotent_requests (scope, key, fingerprint) VALUES ($1, $2, $3)
				RETURNING created_at`, scope, key, fp).Scan(&claimedAt)
		case err != nil:
			return err
		case !bytes.Equal(prevFP, fp):
			return newError(http.StatusUnprocessableEntity, "idempotency_key_reused",
				"this Idempotency-Key was used for a different request")
		case running:
			return errInProgress
		case status != nil:
			stored = &storedResponse{status: int(*status), body: body}
			return nil
		default:
			// The request that claimed the key never finished: take it over.
			claimedAt = createdAt
			_, err = tx.Exec(ctx, `
				UPDATE idempotent_requests SET updated_at = now()
				WHERE scope = $1 AND key = $2 AND created_at = $3`, scope, key, createdAt)
			return err
		}
	})
	return claimedAt, stored, err
}

// writeMargin is how long before the server's write timeout a request with
// an Idempotency-Key times out, which leaves time to write its 503.
const writeMargin = 5 * time.Second

// handlerTimeout is how long a request with an Idempotency-Key can run: at
// most half its key's lease, so a slow one stops before another request
// could take the key over, and writeMargin less than the server's write
// timeout, so its timeout is answered before the connection is cut.
func handlerTimeout(writeTimeout time.Duration) (time.Duration, error) {
	d := claimLease / 2
	if writeTimeout > 0 {
		d = min(d, writeTimeout-writeMargin)
	}
	if d <= 0 {
		return 0, fmt.Errorf("a write timeout of %s leaves requests no time: it must be over %s", writeTimeout, writeMargin)
	}
	return d, nil
}

var errInProgress = newError(http.StatusConflict, "request_in_progress",
	"a request with this Idempotency-Key is still running; retry later")

// finish stores the response, or gives the key back when the request may
// not have had its effect, after a server error or when its client went
// away, so it can be retried. Running it again is safe: the key also goes
// down as the ledger's idempotency key or the order's client order ID.
func (s *Server) finish(ctx context.Context, scope, key string, claimedAt time.Time, rec *bufferedResponse) error {
	var err error
	if rec.status >= http.StatusInternalServerError || rec.status == statusClientClosed {
		_, err = s.cfg.Pool.Exec(ctx, `
			DELETE FROM idempotent_requests WHERE scope = $1 AND key = $2 AND created_at = $3`,
			scope, key, claimedAt)
	} else {
		_, err = s.cfg.Pool.Exec(ctx, `
			UPDATE idempotent_requests SET status = $4, body = $5, updated_at = now()
			WHERE scope = $1 AND key = $2 AND created_at = $3`,
			scope, key, claimedAt, rec.status, rec.body.Bytes())
	}
	return err
}

// bufferedResponse holds a response until it's been stored.
type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (b *bufferedResponse) Header() http.Header         { return b.header }
func (b *bufferedResponse) Write(p []byte) (int, error) { return b.body.Write(p) }
func (b *bufferedResponse) WriteHeader(status int)      { b.status = status }

// MaintainIdempotency creates the partitions for today and the next two
// days, and drops the ones older than the keys are kept. Run it at start
// and then hourly; it's safe to run from several servers at once.
func MaintainIdempotency(ctx context.Context, pool *pgxpool.Pool, now time.Time) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('idempotent_requests partitions', 0))`); err != nil {
			return err
		}
		today := now.UTC().Truncate(24 * time.Hour)
		for i := range 3 {
			day := today.AddDate(0, 0, i)
			sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF idempotent_requests FOR VALUES FROM ('%s') TO ('%s')`,
				partitionName(day), day.Format(time.RFC3339), day.AddDate(0, 0, 1).Format(time.RFC3339))
			if _, err := tx.Exec(ctx, sql); err != nil {
				return fmt.Errorf("create partition for %s: %w", day.Format(time.DateOnly), err)
			}
		}

		rows, err := tx.Query(ctx, `
			SELECT c.relname FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class p ON p.oid = i.inhparent
			WHERE p.relname = 'idempotent_requests'`)
		if err != nil {
			return err
		}
		names, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, name := range names {
			day, parseErr := time.Parse("20060102", strings.TrimPrefix(name, "idempotent_requests_"))
			if parseErr != nil {
				continue // not one of ours
			}
			if day.AddDate(0, 0, 1).Add(keyRetention).Before(now) {
				if _, err = tx.Exec(ctx, `DROP TABLE `+pgx.Identifier{name}.Sanitize()); err != nil {
					return fmt.Errorf("drop partition %s: %w", name, err)
				}
			}
		}
		return nil
	})
}

func partitionName(day time.Time) string {
	return "idempotent_requests_" + day.Format("20060102")
}
