// Package api is brokerd's HTTP API.
package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AmrMady/brokerd/internal/engine"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/portfolio"
)

// Config configures a Server. Without a pool and an engine, it only serves
// market data.
type Config struct {
	Pool   *pgxpool.Pool
	Engine *engine.Engine
	Quotes portfolio.Quotes
	Bars   Bars
	Logger *slog.Logger
}

// Bars gives the most recent bars of a symbol.
type Bars interface {
	Bars(symbol string, interval time.Duration, limit int) []marketdata.Bar
}

// Server serves the API. Other handlers, like the market data stream, can be
// mounted on it with Handle so they share its request IDs and logs.
type Server struct {
	cfg      Config
	mux      *http.ServeMux
	draining atomic.Bool
}

// maxBody is the largest request body the API reads.
const maxBody = 1 << 20

// New returns a Server with the API's routes.
func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{cfg: cfg, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("GET /readyz", s.readyz)
	// Anything else gets the usual JSON error rather than the mux's text.
	s.route("/", func(http.ResponseWriter, *http.Request) error { return errNotFound })
	if cfg.Quotes != nil && cfg.Bars != nil {
		s.route("GET /v1/quotes/{symbol}", s.quote)
		s.route("GET /v1/bars/{symbol}", s.bars)
	}
	if cfg.Pool == nil || cfg.Engine == nil {
		return s
	}
	// Every request that changes something can be retried safely with an
	// Idempotency-Key.
	s.route("POST /v1/accounts", s.idempotent(s.createAccount))
	s.route("GET /v1/accounts/{account}", s.authorized(s.getAccount))
	s.route("POST /v1/accounts/{account}/deposits", s.authorized(s.idempotent(s.deposit)))
	s.route("POST /v1/accounts/{account}/withdrawals", s.authorized(s.idempotent(s.withdraw)))
	s.route("POST /v1/accounts/{account}/orders", s.authorized(s.idempotent(s.placeOrder)))
	s.route("GET /v1/accounts/{account}/orders", s.authorized(s.listOrders))
	s.route("GET /v1/accounts/{account}/orders/{order}", s.authorized(s.getOrder))
	s.route("DELETE /v1/accounts/{account}/orders/{order}", s.authorized(s.idempotent(s.cancelOrder)))
	s.route("GET /v1/accounts/{account}/positions", s.authorized(s.positions))
	s.route("GET /v1/accounts/{account}/activities", s.authorized(s.activities))
	return s
}

// Handle mounts h on the server.
func (s *Server) Handle(pattern string, h http.Handler) {
	s.mux.Handle(pattern, h)
}

// Drain makes the server report that it isn't ready, so a load balancer
// stops sending it traffic before it shuts down.
func (s *Server) Drain() {
	s.draining.Store(true)
}

type handlerFunc func(w http.ResponseWriter, r *http.Request) error

func (s *Server) route(pattern string, h handlerFunc) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			s.writeError(w, r, err)
		}
	})
}

// ServeHTTP gives each request an ID, recovers from panics, and logs it.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.Header.Get("X-Request-Id")
	if !requestIDPattern.MatchString(id) {
		id = newRequestID()
	}
	w.Header().Set("X-Request-Id", id)
	ctx := context.WithValue(r.Context(), requestIDKey{}, id)
	r = r.WithContext(ctx)

	rec := &recorder{ResponseWriter: w, status: http.StatusOK}
	began := time.Now()
	defer func() {
		if p := recover(); p != nil {
			s.cfg.Logger.Error("panic", "request_id", id, "panic", p, "stack", string(debug.Stack()))
			if !rec.wrote {
				s.writeError(rec, r, fmt.Errorf("panic: %v", p))
			}
		}
		s.cfg.Logger.Info("request", "request_id", id, "method", r.Method, "route", r.Pattern,
			"status", rec.status, "bytes", rec.bytes, "duration", time.Since(began))
	}()
	r.Body = http.MaxBytesReader(rec, r.Body, maxBody)
	s.mux.ServeHTTP(rec, r)
}

type requestIDKey struct{}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func newRequestID() string {
	return rand.Text()
}

func requestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}
	return ""
}

// recorder notes the status and size of a response. Unwrap lets the
// WebSocket handler reach the connection to hijack it.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int
	wrote  bool
}

func (r *recorder) WriteHeader(status int) {
	if !r.wrote {
		r.status, r.wrote = status, true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(b []byte) (int, error) {
	r.wrote = true
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	if s.cfg.Pool == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.cfg.Pool.Ping(ctx); err != nil {
		s.cfg.Logger.Warn("not ready", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "database unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	e := errorFor(err)
	if e.status >= http.StatusInternalServerError {
		s.cfg.Logger.Error("request failed", "request_id", requestID(r.Context()), "error", err)
	}
	var body errorBody
	body.Error.Code, body.Error.Message, body.Error.RequestID = e.code, e.message, requestID(r.Context())
	writeJSON(w, e.status, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) //nolint:errcheck // the client went away; nothing to do
}

// decode reads a JSON body into v. Unknown fields are errors, so a typo in
// a field name isn't silently ignored.
func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return newError(http.StatusRequestEntityTooLarge, "too_large", "request body is over %d bytes", tooBig.Limit)
		}
		return newError(http.StatusBadRequest, "invalid_json", "invalid JSON: %s", err)
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return newError(http.StatusBadRequest, "invalid_json", "invalid JSON: more than one value")
	}
	return nil
}
