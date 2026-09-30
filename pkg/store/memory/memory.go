// Package memory is the in-memory reference store used by tests and benchmarks.
// One mutex guards everything; correctness over speed.
package memory

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

type shardKey struct {
	pool string
	txid chainhash.Hash
}

type lease struct {
	holder  string
	expires time.Time
}

// Store is an in-memory store.Store.
type Store struct {
	now    func() time.Time
	mu     sync.Mutex
	shards map[shardKey]*store.Shard
	byTxID map[chainhash.Hash]string // txid -> owning pool; enforces global txid uniqueness
	rows   map[store.Outpoint]*store.Row
	// rowsByShard indexes a shard's row outpoints so ListAvailable and RetireShard
	// don't have to scan every row in the store.
	rowsByShard map[chainhash.Hash][]store.Outpoint
	leases      map[string]lease
}

// Option configures New.
type Option func(*Store)

// WithClock injects the clock used for timestamps and lease expiry.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// New returns an empty store.
func New(opts ...Option) *Store {
	s := &Store{
		now:         time.Now,
		shards:      map[shardKey]*store.Shard{},
		byTxID:      map[chainhash.Hash]string{},
		rows:        map[store.Outpoint]*store.Row{},
		rowsByShard: map[chainhash.Hash][]store.Outpoint{},
		leases:      map[string]lease{},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

var _ store.Store = (*Store)(nil)

// Ping implements store.Store.
func (s *Store) Ping(context.Context) error { return nil }

// Capabilities implements store.Store.
func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: 1, MaxBatch: 1000, AtomicBatch: true, MultiProcess: false}
}

// Close implements store.Store; it is idempotent.
func (s *Store) Close() error { return nil }

func cloneShard(sh *store.Shard, withBEEF bool) store.Shard {
	c := *sh
	if withBEEF {
		c.BEEF = bytes.Clone(sh.BEEF)
	} else {
		c.BEEF = nil
	}
	return c
}

func cloneRow(r *store.Row) store.Row {
	c := *r
	c.LockingScript = bytes.Clone(r.LockingScript)
	c.UnlockingScript = bytes.Clone(r.UnlockingScript)
	if r.SpentHint != nil {
		h := *r.SpentHint
		c.SpentHint = &h
	}
	if r.SpentBy != nil {
		h := *r.SpentBy
		c.SpentBy = &h
	}
	return c
}

func lessShard(a, b *store.Shard) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return bytes.Compare(a.TxID[:], b.TxID[:]) < 0
}
