package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// canceledPool is a PoolService whose Reserve always reports that the client
// went away, regardless of the request context passed to it.
type canceledPool struct{ name string }

func (c canceledPool) Config() pool.Config { return pool.Config{Name: c.name, MaxCount: 10} }

func (c canceledPool) Reserve(context.Context, pool.ReserveRequest) (*pool.Reservation, error) {
	return nil, context.Canceled
}

func (c canceledPool) Release(context.Context, store.Token, []store.Outpoint) (int, error) {
	return 0, nil
}

func (c canceledPool) Spend(context.Context, store.Token, []store.Outpoint, chainhash.Hash) (int, error) {
	return 0, nil
}

func (c canceledPool) Stats(context.Context) (store.Stats, error) { return store.Stats{}, nil }

func (c canceledPool) BEEF(context.Context, chainhash.Hash) ([]byte, int64, store.ShardStatus, error) {
	return nil, 0, 0, nil
}

func (c canceledPool) Ready(context.Context) bool { return true }

// TestClientCancellation exercises the 499 path directly: driving a genuinely
// pre-cancelled client context through httptest.Server is unreliable (the
// server may finish the handler before the client observes the cancellation),
// so this calls the handler in-process instead.
func TestClientCancellation(t *testing.T) {
	p := canceledPool{name: "main"}
	s := &server{
		pools:  []PoolService{p},
		byName: map[string]PoolService{"main": p},
		log:    slog.Default(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/pools/main/reserve", strings.NewReader(`{"count":1}`))
	req.SetPathValue("pool", "main")
	rec := httptest.NewRecorder()

	s.reserve(rec, req)

	assert.Equal(t, 499, rec.Code)
}

// TestBeefURLEscapesPoolName guards item 12: pool names are only length
// bounded (store.ValidatePool), not charset restricted, so beefURL must not
// assume a name is already URL-safe.
func TestBeefURLEscapesPoolName(t *testing.T) {
	got := beefURL("weird pool/name", chainhash.Hash{})
	assert.NotContains(t, got, " ")
	assert.Contains(t, got, "weird%20pool%2Fname")
}
