// Package api implements the FuelKeeper HTTP API: bearer-key auth, health and
// readiness probes, Prometheus metrics, and the /v1/ pool surface (reserve,
// release, spend, BEEF and the admin endpoints) over pkg/pool.Pool.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// PoolService is what the handlers need from pool.Pool.
type PoolService interface {
	Config() pool.Config
	Reserve(ctx context.Context, req pool.ReserveRequest) (*pool.Reservation, error)
	Release(ctx context.Context, token store.Token, ops []store.Outpoint) (int, error)
	Spend(ctx context.Context, token store.Token, ops []store.Outpoint, txid chainhash.Hash) (int, error)
	Stats(ctx context.Context) (store.Stats, error)
	BEEF(ctx context.Context, txid chainhash.Hash) ([]byte, int64, store.ShardStatus, error)
	Ready(ctx context.Context) bool
}

// defaultMaxBodyBytes is Deps.MaxBodyBytes's default.
const defaultMaxBodyBytes = 1 << 20 // 1 MiB

// defaultRequestTimeout is Deps.RequestTimeout's default. It must be set
// larger than every configured pool's InstanceConfig.ReserveTimeout: Reserve
// applies its own, shorter internal timeout and returns a proper
// exhausted/insufficient/unavailable error when it fires, whereas this
// middleware timeout is a last-resort bound on the whole request (including
// Release/Spend/Stats/BEEF/admin calls, which have no timeout of their own).
// If the two were equal or inverted, the middleware could cut a Reserve call
// off before its own timeout had a chance to produce a meaningful error.
const defaultRequestTimeout = 10 * time.Second

// defaultReadHeaderTimeout guards NewServer's *http.Server against slow-header
// (Slowloris-style) clients.
const defaultReadHeaderTimeout = 5 * time.Second

// Deps are the server's dependencies.
type Deps struct {
	Pools          []PoolService // ordered as configured
	Store          store.Store   // admin endpoints and readiness
	Log            *slog.Logger
	APIKeys        []string
	OpenAPI        []byte
	MaxBodyBytes   int64         // default 1 MiB
	RequestTimeout time.Duration // default 10 s; must exceed the pools' ReserveTimeout
	Version        string
}

// server holds the resolved dependencies the route handlers close over.
type server struct {
	pools  []PoolService
	byName map[string]PoolService
	store  store.Store
	log    *slog.Logger
}

// pool looks up a configured pool by name, or pool.ErrUnknownPool.
func (s *server) pool(name string) (PoolService, error) {
	p, ok := s.byName[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", pool.ErrUnknownPool, name)
	}
	return p, nil
}

// NewHandler builds the FuelKeeper HTTP API router: unauthenticated
// healthz/readyz/metrics/openapi.yaml, and the /v1/ API surface behind
// requireAPIKey. Unknown /v1/ paths answer a JSON 404 and known paths called
// with the wrong method answer a JSON 405, matching every other error
// response's {"code","message"} envelope instead of net/http's default
// plain-text bodies.
func NewHandler(d Deps) http.Handler {
	log := d.Log
	if log == nil {
		log = slog.Default()
	}
	maxBody := d.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBodyBytes
	}
	timeout := d.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	byName := make(map[string]PoolService, len(d.Pools))
	for _, p := range d.Pools {
		byName[p.Config().Name] = p
	}
	s := &server{pools: d.Pools, byName: byName, store: d.Store, log: log}

	v1 := http.NewServeMux()
	addRoute(v1, http.MethodGet, "/v1/pools", s.listPools)
	addRoute(v1, http.MethodGet, "/v1/pools/{pool}", s.getPool)
	addRoute(v1, http.MethodPost, "/v1/pools/{pool}/reserve", s.reserve)
	addRoute(v1, http.MethodPost, "/v1/pools/{pool}/release", s.release)
	addRoute(v1, http.MethodPost, "/v1/pools/{pool}/spend", s.spend)
	addRoute(v1, http.MethodGet, "/v1/pools/{pool}/beef/{txid}", s.beef)
	addRoute(v1, http.MethodGet, "/v1/pools/{pool}/shards", s.listShards)
	addRoute(v1, http.MethodPost, "/v1/pools/{pool}/shards/{txid}/retire", s.retireShard)
	addRoute(v1, http.MethodGet, "/v1/pools/{pool}/quarantined", s.listQuarantined)
	addRoute(v1, http.MethodPost, "/v1/pools/{pool}/quarantined/requeue", s.requeue)
	// Least specific: anything under /v1/ that didn't match one of the exact
	// routes above (or their same-path wrong-method fallback registered by
	// addRoute) is a genuinely unknown path.
	v1.HandleFunc("/v1/", notFoundHandler)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", readyz(d.Store, d.Pools))
	mux.Handle("GET /metrics", metricsHandler())
	mux.HandleFunc("GET /openapi.yaml", openapiHandler(d.OpenAPI))
	mux.Handle("/v1/", requireAPIKey(d.APIKeys, v1))

	var h http.Handler = mux
	h = http.MaxBytesHandler(h, maxBody)
	h = withTimeout(timeout, h)
	h = withLogging(log, h)
	h = withRecover(log, h)
	return h
}

// addRoute registers h for method+path, plus a same-path fallback that
// answers any other method on that exact path with a JSON 405. Because the
// method-specific pattern is strictly more specific, it always wins for the
// method it names; the bare pattern only ever receives requests using some
// other method (see net/http's ServeMux precedence rules), so it can reply
// 405 unconditionally.
func addRoute(mux *http.ServeMux, method, path string, h http.HandlerFunc) {
	mux.HandleFunc(method+" "+path, h)
	mux.HandleFunc(path, methodNotAllowedHandler)
}

func methodNotAllowedHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusMethodNotAllowed, wire.Error{Code: "method_not_allowed", Message: "method not allowed"})
}

func notFoundHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotFound, wire.Error{Code: "not_found", Message: "not found"})
}

// statusWriter records the status code written, for logging.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// withRecover converts a panic in any inner handler into a JSON 500 instead
// of crashing the server or leaking a bare stack trace to the client.
func withRecover(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic recovered", "err", rec, "path", r.URL.Path, "method", r.Method)
				writeError(w, errors.New("panic"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// withLogging logs every request at Debug level, skipping the
// operator/scraper endpoints that would otherwise dominate the log.
func withLogging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics", "/healthz", "/readyz":
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Debug("request", "method", r.Method, "path", r.URL.Path, "status", sw.status, "duration", time.Since(start))
	})
}

// withTimeout bounds every request by wrapping r.Context() with
// context.WithTimeout: handlers that pass the context through to
// Release/Spend/Stats/BEEF/admin store calls (Reserve has its own, shorter
// internal timeout) see ctx.Err() once the deadline passes, which mapErr
// turns into a 503 unavailable.
func withTimeout(timeout time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// NewServer builds an *http.Server serving h, with ReadHeaderTimeout set to
// guard against slow-header clients.
func NewServer(listen string, h http.Handler, readTimeout, writeTimeout, idleTimeout time.Duration) *http.Server {
	return &http.Server{
		Addr:              listen,
		Handler:           h,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
	}
}

// decode JSON-decodes r.Body into v, rejecting unknown fields and mapping
// body-size/format failures to a friendly ErrBadRequest.
func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxErr):
			return fmt.Errorf("%w: body too large", pool.ErrBadRequest)
		case errors.Is(err, io.EOF):
			return fmt.Errorf("%w: empty body", pool.ErrBadRequest)
		default:
			return fmt.Errorf("%w: invalid json: %w", pool.ErrBadRequest, err)
		}
	}
	return nil
}
