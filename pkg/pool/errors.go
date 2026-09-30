package pool

import "errors"

var (
	// ErrBadRequest is a caller error (400).
	ErrBadRequest = errors.New("pool: bad request")
	// ErrUnknownPool names a pool that is not configured (404).
	ErrUnknownPool = errors.New("pool: unknown pool")
	// ErrExhausted means no fuel is available right now (503 + Retry-After).
	ErrExhausted = errors.New("pool: exhausted")
	// ErrInsufficient means fewer than min_count outputs could be reserved (409 + Retry-After).
	ErrInsufficient = errors.New("pool: insufficient")
	// ErrBackpressure means the coalescer queue is full (503 + Retry-After).
	ErrBackpressure = errors.New("pool: backpressure")
	// ErrUnavailable means the store failed after retries (503).
	ErrUnavailable = errors.New("pool: store unavailable")
)
