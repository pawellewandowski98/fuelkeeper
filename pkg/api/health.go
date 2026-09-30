package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
)

// Pinger is the minimal store dependency readyz needs.
type Pinger interface {
	Ping(ctx context.Context) error
}

// readinessPingTimeout bounds how long readyz waits on the store ping.
const readinessPingTimeout = 2 * time.Second

// healthz reports process liveness unconditionally: it never touches the
// store or a pool, so it always answers as long as the process is running.
func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz reports whether the service can usefully serve traffic: at least one
// pool must be configured and the store must answer a ping. PoolsReady counts
// how many configured pools report Ready(ctx) == true, so an operator can
// alert on 0 even when the overall response is 200 "ready" (a fresh instance
// that has not yet adopted a shard is still able to serve, just not primed).
//
// A ping failure's real error (which may embed a DSN, host, user or SQL
// state) is logged at Warn, not returned to the client: the response's error
// field is always the fixed string "store ping failed".
func readyz(log *slog.Logger, p Pinger, pools []PoolService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(pools) == 0 {
			writeJSON(w, http.StatusServiceUnavailable, wire.Ready{Status: "no pools configured"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), readinessPingTimeout)
		defer cancel()
		if err := p.Ping(ctx); err != nil {
			log.Warn("readyz: store ping failed", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, wire.Ready{
				Status: "store unavailable",
				Pools:  len(pools),
				Error:  "store ping failed",
			})
			return
		}
		ready := 0
		for _, pl := range pools {
			if pl.Ready(r.Context()) {
				ready++
			}
		}
		writeJSON(w, http.StatusOK, wire.Ready{Status: "ready", Pools: len(pools), PoolsReady: ready})
	}
}

// metricsHandler exposes the process's Prometheus metrics.
func metricsHandler() http.Handler { return promhttp.Handler() }

// openapiHandler serves the embedded OpenAPI document as YAML.
func openapiHandler(doc []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(doc)
	}
}
