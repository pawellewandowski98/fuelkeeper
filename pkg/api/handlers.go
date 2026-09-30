package api

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

const (
	// defaultListLimit is applied when a caller omits (or zeroes) a limit
	// query parameter on an admin listing endpoint.
	defaultListLimit = 100
	// maxListLimit caps every admin listing endpoint's limit parameter.
	maxListLimit = 1000
	// maxRequeueOutpoints caps how many outpoints one requeue call accepts.
	maxRequeueOutpoints = 1000
	// quarantineHorizon is how far into the future listQuarantined's
	// exclusive "before" cutoff is set, so the call returns every row
	// quarantined up to now regardless of clock skew between the caller and
	// the store's clock.
	quarantineHorizon = 24 * time.Hour
)

// maxReserveTTLSeconds is the largest ttl_seconds reserve accepts: beyond
// this, converting to a time.Duration (seconds * time.Second) would overflow
// int64 and wrap into an unpredictable (possibly negative) duration.
const maxReserveTTLSeconds = math.MaxInt64 / int64(time.Second)

// listPools returns every configured pool with its live store stats.
func (s *server) listPools(w http.ResponseWriter, r *http.Request) {
	out := make([]wire.Pool, 0, len(s.pools))
	for _, p := range s.pools {
		stats, err := p.Stats(r.Context())
		if err != nil {
			s.writeError(w, err)
			return
		}
		out = append(out, toPool(p.Config(), stats))
	}
	writeJSON(w, http.StatusOK, out)
}

// getPool returns one configured pool with its live store stats.
func (s *server) getPool(w http.ResponseWriter, r *http.Request) {
	p, err := s.pool(r.PathValue("pool"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	stats, err := p.Stats(r.Context())
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toPool(p.Config(), stats))
}

// reserve asks a pool for fuel.
func (s *server) reserve(w http.ResponseWriter, r *http.Request) {
	poolName := r.PathValue("pool")
	p, err := s.pool(poolName)
	if err != nil {
		s.writeError(w, err)
		return
	}
	var req wire.ReserveRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	if req.Count < 0 || req.MinCount < 0 || req.TTLSeconds < 0 {
		s.writeError(w, fmt.Errorf("%w: count, min_count and ttl_seconds must be >= 0", pool.ErrBadRequest))
		return
	}
	if int64(req.TTLSeconds) > maxReserveTTLSeconds {
		s.writeError(w, fmt.Errorf("%w: ttl_seconds must be <= %d", pool.ErrBadRequest, maxReserveTTLSeconds))
		return
	}
	res, err := p.Reserve(r.Context(), pool.ReserveRequest{
		Count:    req.Count,
		MinCount: req.MinCount,
		TTL:      time.Duration(req.TTLSeconds) * time.Second,
	})
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toReserveResponse(poolName, res))
}

// release returns reserved outputs to a pool.
func (s *server) release(w http.ResponseWriter, r *http.Request) {
	p, err := s.pool(r.PathValue("pool"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	var req wire.ReleaseRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	tok, err := parseToken(req.Token)
	if err != nil {
		s.writeError(w, err)
		return
	}
	ops, err := parseOutpoints(req.Outpoints, p.Config().MaxCount)
	if err != nil {
		s.writeError(w, err)
		return
	}
	n, err := p.Release(r.Context(), tok, ops)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, wire.CountResponse{Released: n})
}

// spend reports outputs spent in a txid.
func (s *server) spend(w http.ResponseWriter, r *http.Request) {
	p, err := s.pool(r.PathValue("pool"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	var req wire.SpendRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	tok, err := parseToken(req.Token)
	if err != nil {
		s.writeError(w, err)
		return
	}
	ops, err := parseOutpoints(req.Outpoints, p.Config().MaxCount)
	if err != nil {
		s.writeError(w, err)
		return
	}
	txid, err := parseTxID(req.TxID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	n, err := p.Spend(r.Context(), tok, ops, txid)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, wire.CountResponse{Accepted: n})
}

// beef serves a shard's BEEF, cacheable once mined and revalidatable via ETag.
func (s *server) beef(w http.ResponseWriter, r *http.Request) {
	p, err := s.pool(r.PathValue("pool"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	txid, err := parseTxID(r.PathValue("txid"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	beef, version, status, err := p.BEEF(r.Context(), txid)
	if err != nil {
		s.writeError(w, err)
		return
	}
	etag := fmt.Sprintf("%q", "v"+strconv.FormatInt(version, 10))
	w.Header().Set("ETag", etag)
	w.Header().Set("X-Shard-Status", status.String())
	if status == store.ShardMined {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && ifNoneMatchHit(inm, etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(beef)
}

// listShards is an admin view of one pool's shards.
func (s *server) listShards(w http.ResponseWriter, r *http.Request) {
	poolName := r.PathValue("pool")
	if _, err := s.pool(poolName); err != nil {
		s.writeError(w, err)
		return
	}
	var f store.ShardFilter
	if raw := r.URL.Query().Get("status"); raw != "" {
		st, ok := parseShardStatus(raw)
		if !ok {
			s.writeError(w, fmt.Errorf("%w: unknown status %q", pool.ErrBadRequest, raw))
			return
		}
		f.Statuses = []store.ShardStatus{st}
	}
	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	f.Limit = limit
	shards, err := s.store.ListShards(r.Context(), poolName, f)
	if err != nil {
		s.writeError(w, err)
		return
	}
	out := make([]wire.ShardInfo, 0, len(shards))
	for _, sh := range shards {
		out = append(out, toShardInfo(sh))
	}
	writeJSON(w, http.StatusOK, out)
}

// retireShard marks a shard invalid and invalidates its non-terminal rows.
func (s *server) retireShard(w http.ResponseWriter, r *http.Request) {
	poolName := r.PathValue("pool")
	if _, err := s.pool(poolName); err != nil {
		s.writeError(w, err)
		return
	}
	txid, err := parseTxID(r.PathValue("txid"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	n, err := s.store.RetireShard(r.Context(), poolName, txid)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.log.Warn("shard retired via admin API", "pool", poolName, "txid", txid.String(), "rows_invalidated", n)
	writeJSON(w, http.StatusOK, wire.CountResponse{Retired: n})
}

// listQuarantined is an admin view of one pool's quarantined rows.
func (s *server) listQuarantined(w http.ResponseWriter, r *http.Request) {
	poolName := r.PathValue("pool")
	if _, err := s.pool(poolName); err != nil {
		s.writeError(w, err)
		return
	}
	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	rows, err := s.store.ListQuarantined(r.Context(), poolName, time.Now().Add(quarantineHorizon), nil, limit)
	if err != nil {
		s.writeError(w, err)
		return
	}
	out := make([]wire.Row, 0, len(rows))
	for _, row := range rows {
		out = append(out, toRow(row))
	}
	writeJSON(w, http.StatusOK, out)
}

// requeue moves quarantined outpoints back to available.
func (s *server) requeue(w http.ResponseWriter, r *http.Request) {
	poolName := r.PathValue("pool")
	if _, err := s.pool(poolName); err != nil {
		s.writeError(w, err)
		return
	}
	var req wire.OutpointsRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	ops, err := parseOutpoints(req.Outpoints, maxRequeueOutpoints)
	if err != nil {
		s.writeError(w, err)
		return
	}
	n, err := s.store.Resolve(r.Context(), poolName, ops, store.StateAvailable, nil)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.log.Warn("outpoints requeued via admin API", "pool", poolName, "count", n)
	writeJSON(w, http.StatusOK, wire.CountResponse{Requeued: n})
}

// ifNoneMatchHit reports whether header (an If-None-Match value, possibly a
// comma-separated list) matches etag under RFC 9110 §13.1.2's weak
// comparison: "*" always matches, and each comma-separated member matches
// when its opaque-tag equals etag's after stripping an optional leading "W/"
// weak indicator. etag itself is always a strong tag (a quoted "v<version>",
// see beef), so it is compared as-is.
func ifNoneMatchHit(header, etag string) bool {
	header = strings.TrimSpace(header)
	if header == "*" {
		return true
	}
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		part = strings.TrimPrefix(part, "W/")
		if part == etag {
			return true
		}
	}
	return false
}

// parseShardStatus maps the wire status filter to a store.ShardStatus.
func parseShardStatus(s string) (store.ShardStatus, bool) {
	switch s {
	case "unmined":
		return store.ShardUnmined, true
	case "mined":
		return store.ShardMined, true
	case "retired":
		return store.ShardRetired, true
	case "invalid":
		return store.ShardInvalid, true
	default:
		return 0, false
	}
}

// parseLimit parses an admin listing's limit query parameter: empty or 0
// falls back to defaultListLimit, negative or non-numeric is a bad request,
// and anything above maxListLimit is clamped down to it.
func parseLimit(raw string) (int, error) {
	if raw == "" {
		return defaultListLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: invalid limit %q", pool.ErrBadRequest, raw)
	}
	if n == 0 {
		return defaultListLimit, nil
	}
	if n > maxListLimit {
		n = maxListLimit
	}
	return n, nil
}
