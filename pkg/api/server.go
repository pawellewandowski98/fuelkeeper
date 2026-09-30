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
	"runtime/debug"
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
//
// Auth: APIKeys gates every /v1/ endpoint, including the four admin
// endpoints (list/retire shards, list/requeue quarantined rows) by default.
// AdminKeys, when non-empty, replaces APIKeys as the gate for just those four
// admin endpoints: a caller must then present a key from AdminKeys there, and
// a plain APIKeys key is rejected with 401. With both APIKeys and AdminKeys
// left empty, the entire API is open (no auth at all).
type Deps struct {
	Pools          []PoolService // ordered as configured
	Store          store.Store   // admin endpoints and readiness
	Log            *slog.Logger
	APIKeys        []string // gates every /v1/ endpoint; empty means open
	AdminKeys      []string // gates the 4 admin endpoints instead of APIKeys when non-empty
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

// writeError logs the real error for the response codes that hide it from
// the client (writeError itself replaces "unavailable" and "internal"
// messages with a fixed, safe string; see writeError) and then writes the
// sanitized envelope. Every other code (bad_request, not_found, ...) is
// routine and is not logged here.
func (s *server) writeError(w http.ResponseWriter, err error) {
	_, code, _ := mapErr(err)
	switch code {
	case "internal":
		s.log.Error("request failed", "err", err)
	case "unavailable":
		s.log.Warn("request failed", "err", err)
	}
	writeError(w, err)
}

// Route names one endpoint's HTTP method and net/http.ServeMux path pattern
// (e.g. "/v1/pools/{pool}").
type Route struct {
	Method  string
	Pattern string
}

// topRoutes are the unauthenticated operator endpoints.
var topRoutes = []Route{
	{Method: http.MethodGet, Pattern: "/healthz"},
	{Method: http.MethodGet, Pattern: "/readyz"},
	{Method: http.MethodGet, Pattern: "/metrics"},
	{Method: http.MethodGet, Pattern: "/openapi.yaml"},
}

// v1Route is one /v1/ endpoint: its Route, whether it is gated by AdminKeys
// (falling back to APIKeys when AdminKeys is empty) rather than always
// APIKeys, and the *server method that serves it.
type v1Route struct {
	Route
	admin   bool
	handler func(*server, http.ResponseWriter, *http.Request)
}

// v1Routes is the full /v1/ surface; NewHandler registers a ServeMux route
// (plus its OPTIONS/405 fallback, see addRoute) from each entry, and Routes
// exposes the same table's Route half for tooling.
var v1Routes = []v1Route{
	{Route{Method: http.MethodGet, Pattern: "/v1/pools"}, false, (*server).listPools},
	{Route{Method: http.MethodGet, Pattern: "/v1/pools/{pool}"}, false, (*server).getPool},
	{Route{Method: http.MethodPost, Pattern: "/v1/pools/{pool}/reserve"}, false, (*server).reserve},
	{Route{Method: http.MethodPost, Pattern: "/v1/pools/{pool}/release"}, false, (*server).release},
	{Route{Method: http.MethodPost, Pattern: "/v1/pools/{pool}/spend"}, false, (*server).spend},
	{Route{Method: http.MethodGet, Pattern: "/v1/pools/{pool}/beef/{txid}"}, false, (*server).beef},
	{Route{Method: http.MethodGet, Pattern: "/v1/pools/{pool}/shards"}, true, (*server).listShards},
	{Route{Method: http.MethodPost, Pattern: "/v1/pools/{pool}/shards/{txid}/retire"}, true, (*server).retireShard},
	{Route{Method: http.MethodGet, Pattern: "/v1/pools/{pool}/quarantined"}, true, (*server).listQuarantined},
	{Route{Method: http.MethodPost, Pattern: "/v1/pools/{pool}/quarantined/requeue"}, true, (*server).requeue},
}

// Routes lists every HTTP method+path pattern NewHandler serves, including
// the unauthenticated operator endpoints, so tooling (e.g. an OpenAPI
// coverage test) can compare the two in both directions.
func Routes() []Route {
	out := make([]Route, 0, len(topRoutes)+len(v1Routes))
	out = append(out, topRoutes...)
	for _, r := range v1Routes {
		out = append(out, r.Route)
	}
	return out
}

// topHandlers resolves each topRoutes entry to its handler. Unlike v1Routes,
// this can't be a package-level table: readyz and openapi.yaml close over
// configuration only known once NewHandler runs.
func topHandlers(log *slog.Logger, d Deps) map[Route]http.HandlerFunc {
	return map[Route]http.HandlerFunc{
		{Method: http.MethodGet, Pattern: "/healthz"}:      healthz,
		{Method: http.MethodGet, Pattern: "/readyz"}:       readyz(log, d.Store, d.Pools),
		{Method: http.MethodGet, Pattern: "/metrics"}:      metricsHandler().ServeHTTP,
		{Method: http.MethodGet, Pattern: "/openapi.yaml"}: openapiHandler(d.OpenAPI),
	}
}

// NewHandler builds the FuelKeeper HTTP API router from Routes(): the
// unauthenticated operator endpoints and the /v1/ API surface, the latter
// gated per-route by APIKeys or (for the 4 admin endpoints, when configured)
// AdminKeys. Unknown /v1/ paths (and unknown top-level paths) answer a JSON
// 404; a known path called with an unsupported method answers a JSON 405
// with Allow set; OPTIONS on a known path answers 204 with Allow set and no
// body — matching every other response's {"code","message"} envelope instead
// of net/http's default plain-text bodies.
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

	fuelAuth := func(h http.HandlerFunc) http.HandlerFunc { return requireAPIKey(d.APIKeys, h) }
	adminKeys := d.AdminKeys
	if len(adminKeys) == 0 {
		adminKeys = d.APIKeys
	}
	adminAuth := func(h http.HandlerFunc) http.HandlerFunc { return requireAPIKey(adminKeys, h) }

	v1 := http.NewServeMux()
	for _, e := range v1Routes {
		h := func(w http.ResponseWriter, r *http.Request) { e.handler(s, w, r) }
		if e.admin {
			addRoute(v1, e.Method, e.Pattern, adminAuth(h))
		} else {
			addRoute(v1, e.Method, e.Pattern, fuelAuth(h))
		}
	}
	// Least specific: anything under /v1/ that didn't match one of the exact
	// routes above (or their same-path OPTIONS/405 fallback registered by
	// addRoute) is a genuinely unknown path.
	v1.HandleFunc("/v1/", notFoundHandler)

	th := topHandlers(log, d)
	mux := http.NewServeMux()
	for _, rt := range topRoutes {
		addRoute(mux, rt.Method, rt.Pattern, th[rt])
	}
	mux.Handle("/v1/", v1)
	mux.HandleFunc("/", notFoundHandler)

	var h http.Handler = mux
	h = http.MaxBytesHandler(h, maxBody)
	h = withTimeout(timeout, h)
	h = withLogging(log, h)
	h = withRecover(log, h)
	return h
}

// addRoute registers h for method+path, plus a same-path fallback that
// answers OPTIONS with a 204 (and an Allow header) and any other method on
// that exact path with a JSON 405 (also carrying Allow). Because the
// method-specific pattern is strictly more specific, it always wins for the
// method(s) it names; net/http's ServeMux additionally routes HEAD to a GET
// pattern, so a GET route's Allow lists both. The bare fallback pattern
// therefore only ever receives OPTIONS or a genuinely unsupported method (see
// net/http's ServeMux precedence rules).
func addRoute(mux *http.ServeMux, method, path string, h http.HandlerFunc) {
	allow := method
	if method == http.MethodGet {
		allow = "GET, HEAD"
	}
	mux.HandleFunc(method+" "+path, h)
	mux.HandleFunc(path, optionsOrMethodNotAllowed(allow))
}

// optionsOrMethodNotAllowed answers OPTIONS with an empty 204 and everything
// else with a JSON 405, both carrying Allow.
func optionsOrMethodNotAllowed(allow string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", allow)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusMethodNotAllowed, wire.Error{Code: "method_not_allowed", Message: "method not allowed"})
	}
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

// headerTracker records whether a response's status line (or body) has
// already been sent, so a recovered panic knows whether it is still safe to
// write an error response instead of corrupting an already-started one.
type headerTracker struct {
	http.ResponseWriter
	written bool
}

func (h *headerTracker) WriteHeader(status int) {
	h.written = true
	h.ResponseWriter.WriteHeader(status)
}

func (h *headerTracker) Write(p []byte) (int, error) {
	h.written = true
	return h.ResponseWriter.Write(p)
}

// withRecover converts a panic in any inner handler into a JSON 500 instead
// of crashing the server or leaking a bare stack trace to the client. A
// panic with http.ErrAbortHandler is re-panicked unchanged: it is net/http's
// own sentinel for "abort this response silently", and net/http's server
// special-cases it (no stack trace logged, connection simply closed).
func withRecover(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ht := &headerTracker{ResponseWriter: w}
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if rec == http.ErrAbortHandler { //nolint:errorlint // ErrAbortHandler is compared by identity, per net/http's own contract; it is never wrapped
				panic(rec)
			}
			log.Error("panic recovered", "err", rec, "path", r.URL.Path, "method", r.Method, "stack", string(debug.Stack()))
			if !ht.written {
				writeError(w, errors.New("panic"))
			}
		}()
		next.ServeHTTP(ht, r)
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

// decode JSON-decodes r.Body into v, rejecting unknown fields, trailing data
// after the JSON value, and mapping body-size/format failures to a friendly
// ErrBadRequest.
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
	if dec.More() {
		return fmt.Errorf("%w: unexpected data after JSON body", pool.ErrBadRequest)
	}
	return nil
}
