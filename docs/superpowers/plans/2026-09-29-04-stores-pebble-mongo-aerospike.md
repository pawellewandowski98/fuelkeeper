# FuelKeeper Plan 4: Pebble, MongoDB and Aerospike stores Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the three remaining `store.Store` backends behind the same conformance suite: PebbleDB (embedded, single process), MongoDB (shared, no multi-document transactions) and Aerospike (shared, per-record CAS), plus their compose services, build tags, CI wiring and factory entries.

**Architecture:** Nothing changes above the store. Each backend implements the Plan 1 contract with the CAS discipline the spec requires: every mutation re-asserts the row's current state in the same atomic write. Pebble gets a store-wide write mutex and one batch per mutation with maintained counters and index keys; Mongo uses per-document conditional updates (pipeline updates for the conditional reserve stamps) with a follow-up read to classify a batch; Aerospike uses `BatchOperate` with per-record filter expressions and presence-encoded index bins, never generation checks and never client retries on non-idempotent writes.

**Tech Stack:** `github.com/cockroachdb/pebble` v1.1.x, `go.mongodb.org/mongo-driver/v2`, `github.com/aerospike/aerospike-client-go/v8`, testify, docker compose (mongo:7, aerospike CE 7).

**Spec:** `docs/superpowers/specs/2026-09-29-fuelkeeper-design.md` — "Store interface", "Per-backend essentials". Reference implementations: `pkg/store/postgres` and `pkg/store/sqlite` (Plan 1). The suite `pkg/store/storetest.RunSuite` is the acceptance test for every task here.

**How this plan is written.** The three critical operations per backend (`Reserve`, `AcquireShard`, `Quarantine`) and every layout/index decision are given in full. The remaining methods (`GetShard`, `UpdateShard`, `RetireShard`, `RenewShard`, `ReleaseShard`, `ListShards`, `ListAvailable`, `GetRows`, `Release`, `Spend`, `ListQuarantined`, `Resolve`, `Stats`, leases) are specified by their predicates and by pointing at the SQLite/Postgres method they mirror; the suite pins their behaviour. Implementers run the suite after each file and fix until green.

**Model tiering (user preference):** implementers on `sonnet`; spec reviews on `sonnet`; code-quality reviews of each backend's `Reserve`/`Quarantine`/`AcquireShard` on the most capable model (they carry G1).

---

## File structure

```
pkg/store/pebble/     pebble.go (Open, Close, Caps, mutex, batch helpers, counters), keys.go, codec.go (JSON row/shard),
                      shards.go, rows.go, lease.go, pebble_test.go
pkg/store/mongodb/    mongodb.go (New, indexes, write concern), docs.go (BSON structs), shards.go, rows.go, lease.go,
                      mongodb_test.go (tag mongodb), indexes_test.go
pkg/store/aerospike/  aerospike.go (New, indexes, policies), bins.go, shards.go, rows.go, lease.go,
                      aerospike_test.go (tag aerospike), policy_test.go
pkg/store/factory/factory.go   (+pebble, mongodb, aerospike)
deploy/docker-compose.yaml     (+mongo, aerospike)
deploy/aerospike.conf
Makefile, .github/workflows/ci.yml (integration matrix)
```

Shared conventions (same as Plan 1): tx ids stored as raw 32 bytes; times as unix microseconds where the backend has no native timestamp; state/status/lock-mode codes identical; `Caps` as listed per backend.

### Task 1: Pebble backend (`pkg/store/pebble`)

Single process, one write mutex, one batch per mutation. Index keys and per-pool counters are written in the same batch as the row, so they are always consistent with it.

**Key layout** (all binary; big-endian fixed-width so prefixes sort):

| key | value | purpose |
|---|---|---|
| `r/` + txid(32) + vout(4) | JSON row | primary row; a shard's rows are contiguous |
| `s/` + pool + `\x00` + txid(32) | JSON shard (with BEEF) | shard record |
| `is/` + pool + `\x00` + status(1) + created_us(8) + txid(32) | empty | adoption order per status |
| `ie/` + pool + `\x00` + expires_us(8) + txid(32) + vout(4) | empty | reserved rows by expiry |
| `iq/` + pool + `\x00` + quarantined_us(8) + txid(32) + vout(4) | empty | quarantined rows by age |
| `c/` + pool + `\x00` + `rows` + state(1) | int64 LE | row counter per state |
| `c/` + pool + `\x00` + `shards` + status(1) | int64 LE | shard counter per status |
| `l/` + name | JSON lease | job leases |

Every row state transition updates: the row, the `ie`/`iq` index keys for the old and new state, and the two counters. Every shard status change updates the `is` key and shard counters.

**Files:**
- Create: `pkg/store/pebble/pebble.go`, `keys.go`, `codec.go`, `shards.go`, `rows.go`, `lease.go`
- Test: `pkg/store/pebble/pebble_test.go`

- [ ] **Step 1: Write the test**

`pkg/store/pebble/pebble_test.go`:
```go
package pebble_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	pebblestore "github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		s, err := pebblestore.Open(context.Background(), pebblestore.Config{Path: t.TempDir(), SyncWrites: false}, pebblestore.WithClock(now))
		require.NoError(t, err)
		return s
	})
}

func TestReopenKeepsState(t *testing.T) {
	dir := t.TempDir()
	clk := storetest.NewClock(storetest.Start)
	s, err := pebblestore.Open(context.Background(), pebblestore.Config{Path: dir, SyncWrites: true}, pebblestore.WithClock(clk.Now))
	require.NoError(t, err)
	sh, rows := storetest.MintShard(t, s, "main", 3, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", rows[:1], tok, clk.Now().Add(time.Minute))
	require.NoError(t, s.Close())

	s, err = pebblestore.Open(context.Background(), pebblestore.Config{Path: dir}, pebblestore.WithClock(clk.Now))
	require.NoError(t, err)
	defer s.Close()
	st, err := s.Stats(context.Background(), "main")
	require.NoError(t, err)
	require.Equal(t, int64(2), st.Available)
	require.Equal(t, int64(1), st.Reserved)
	got, err := s.GetShard(context.Background(), "main", sh.TxID)
	require.NoError(t, err)
	require.Equal(t, sh.BEEF, got.BEEF)
	avail, err := s.ListAvailable(context.Background(), "main", sh.TxID, 0, 10)
	require.NoError(t, err)
	require.Len(t, avail, 2)
}
```
(Use `SyncWrites: false` in the conformance run for speed; the default in `Config` is `true`.)

- [ ] **Step 2: Run to verify failure**

Run: `cd /git/fuelkeeper && go get github.com/cockroachdb/pebble@v1.1.5 && go test ./pkg/store/pebble/`
Expected: build FAIL.

- [ ] **Step 3: Write `pkg/store/pebble/keys.go`**

```go
package pebble

import (
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func us(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixMicro()) //nolint:gosec // dates after 1970
}

func fromUS(v uint64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMicro(int64(v)).UTC() //nolint:gosec // bounded
}

func rowKey(op store.Outpoint) []byte {
	k := make([]byte, 0, 2+32+4)
	k = append(k, 'r', '/')
	k = append(k, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(k, op.Vout)
}

// rowPrefix is the prefix of every row of one shard.
func rowPrefix(txid chainhash.Hash) []byte {
	k := make([]byte, 0, 2+32)
	k = append(k, 'r', '/')
	return append(k, txid[:]...)
}

func shardKey(pool string, txid chainhash.Hash) []byte {
	k := make([]byte, 0, 2+len(pool)+1+32)
	k = append(k, 's', '/')
	k = append(k, pool...)
	k = append(k, 0)
	return append(k, txid[:]...)
}

func shardPrefix(pool string) []byte {
	k := append([]byte("s/"), pool...)
	return append(k, 0)
}

func adoptKey(pool string, status store.ShardStatus, created time.Time, txid chainhash.Hash) []byte {
	k := append([]byte("is/"), pool...)
	k = append(k, 0, byte(status))
	k = binary.BigEndian.AppendUint64(k, us(created))
	return append(k, txid[:]...)
}

func adoptPrefix(pool string, status store.ShardStatus) []byte {
	k := append([]byte("is/"), pool...)
	return append(k, 0, byte(status))
}

func expiryKey(pool string, at time.Time, op store.Outpoint) []byte {
	k := append([]byte("ie/"), pool...)
	k = append(k, 0)
	k = binary.BigEndian.AppendUint64(k, us(at))
	k = append(k, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(k, op.Vout)
}

func expiryPrefix(pool string) []byte { return append(append([]byte("ie/"), pool...), 0) }

func quarantineKey(pool string, at time.Time, op store.Outpoint) []byte {
	k := append([]byte("iq/"), pool...)
	k = append(k, 0)
	k = binary.BigEndian.AppendUint64(k, us(at))
	k = append(k, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(k, op.Vout)
}

func quarantinePrefix(pool string) []byte { return append(append([]byte("iq/"), pool...), 0) }

// upperBoundBefore returns the exclusive iterator bound for entries with timestamp < before.
func upperBoundBefore(prefix []byte, before time.Time) []byte {
	return binary.BigEndian.AppendUint64(append([]byte{}, prefix...), us(before))
}

func rowCounterKey(pool string, st store.State) []byte {
	k := append([]byte("c/"), pool...)
	return append(append(k, 0), 'r', byte(st))
}

func shardCounterKey(pool string, st store.ShardStatus) []byte {
	k := append([]byte("c/"), pool...)
	return append(append(k, 0), 's', byte(st))
}

func leaseKey(name string) []byte { return append([]byte("l/"), name...) }

// prefixEnd returns the smallest key greater than every key with the prefix.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte{}, prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
```

- [ ] **Step 4: Write `pkg/store/pebble/codec.go`**

```go
package pebble

import (
	"encoding/json"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

type rowDoc struct {
	TxID  []byte `json:"t"`
	Vout  uint32 `json:"v"`
	Pool  string `json:"p"`
	Sats  uint64 `json:"s"`
	Lock  []byte `json:"l"`
	Unl   []byte `json:"u"`
	State uint8  `json:"st"`
	RB    []byte `json:"rb,omitempty"`
	RA    uint64 `json:"ra,omitempty"`
	EA    uint64 `json:"ea,omitempty"`
	QA    uint64 `json:"qa,omitempty"`
	Hint  []byte `json:"h,omitempty"`
	SB    []byte `json:"sb,omitempty"`
	UA    uint64 `json:"ua"`
}

func encodeRow(r *store.Row) []byte {
	d := rowDoc{TxID: r.TxID[:], Vout: r.Vout, Pool: r.Pool, Sats: r.Satoshis, Lock: r.LockingScript, Unl: r.UnlockingScript,
		State: uint8(r.State), RA: us(r.ReservedAt), EA: us(r.ExpiresAt), QA: us(r.QuarantinedAt), UA: us(r.UpdatedAt)}
	if !r.ReservedBy.IsZero() {
		d.RB = r.ReservedBy[:]
	}
	if r.SpentHint != nil {
		d.Hint = r.SpentHint[:]
	}
	if r.SpentBy != nil {
		d.SB = r.SpentBy[:]
	}
	b, _ := json.Marshal(d)
	return b
}

func decodeRow(b []byte) (store.Row, error) {
	var d rowDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return store.Row{}, err
	}
	var r store.Row
	copy(r.TxID[:], d.TxID)
	r.Vout, r.Pool, r.Satoshis, r.LockingScript, r.UnlockingScript = d.Vout, d.Pool, d.Sats, d.Lock, d.Unl
	r.State = store.State(d.State)
	copy(r.ReservedBy[:], d.RB)
	r.ReservedAt, r.ExpiresAt, r.QuarantinedAt, r.UpdatedAt = fromUS(d.RA), fromUS(d.EA), fromUS(d.QA), fromUS(d.UA)
	if len(d.Hint) == 32 {
		h, _ := chainhash.NewHash(d.Hint)
		r.SpentHint = h
	}
	if len(d.SB) == 32 {
		h, _ := chainhash.NewHash(d.SB)
		r.SpentBy = h
	}
	return r, nil
}

type shardDoc struct {
	Pool    string `json:"p"`
	TxID    []byte `json:"t"`
	Size    int    `json:"n"`
	Denom   uint64 `json:"d"`
	Mode    uint8  `json:"m"`
	Status  uint8  `json:"s"`
	Owner   string `json:"o,omitempty"`
	LU      uint64 `json:"lu,omitempty"`
	Version int64  `json:"v"`
	BEEF    []byte `json:"b"`
	CA      uint64 `json:"ca"`
	UA      uint64 `json:"ua"`
}

func encodeShard(s *store.Shard) []byte {
	b, _ := json.Marshal(shardDoc{Pool: s.Pool, TxID: s.TxID[:], Size: s.Size, Denom: s.Denomination, Mode: uint8(s.LockMode), Status: uint8(s.Status),
		Owner: s.Owner, LU: us(s.LeaseUntil), Version: s.Version, BEEF: s.BEEF, CA: us(s.CreatedAt), UA: us(s.UpdatedAt)})
	return b
}

func decodeShard(b []byte) (store.Shard, error) {
	var d shardDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return store.Shard{}, err
	}
	var s store.Shard
	s.Pool, s.Size, s.Denomination, s.Owner, s.Version, s.BEEF = d.Pool, d.Size, d.Denom, d.Owner, d.Version, d.BEEF
	copy(s.TxID[:], d.TxID)
	s.LockMode, s.Status = store.LockMode(d.Mode), store.ShardStatus(d.Status)
	s.LeaseUntil, s.CreatedAt, s.UpdatedAt = fromUS(d.LU), fromUS(d.CA), fromUS(d.UA)
	return s, nil
}

type leaseDoc struct {
	Holder  string `json:"h"`
	Expires uint64 `json:"e"`
}
```

- [ ] **Step 5: Write `pkg/store/pebble/pebble.go`**

```go
// Package pebble implements store.Store on an embedded PebbleDB (single process).
package pebble

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	pdb "github.com/cockroachdb/pebble"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

const maxBatch = 1000

// Config configures Open.
type Config struct {
	Path       string
	SyncWrites bool // default true via Open when Config is zero-valued for this field? No: explicit; see Open.
}

// Option configures Open.
type Option func(*Store)

// WithClock injects the clock.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// Store is a Pebble store.Store.
type Store struct {
	db   *pdb.DB
	wo   *pdb.WriteOptions
	now  func() time.Time
	mu   sync.Mutex // serialises all mutations
	once sync.Once
}

var _ store.Store = (*Store)(nil)

// Open opens (or creates) the database at cfg.Path.
func Open(_ context.Context, cfg Config, opts ...Option) (*Store, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("%w: pebble path is required", store.ErrInvalidArgument)
	}
	db, err := pdb.Open(cfg.Path, &pdb.Options{})
	if err != nil {
		return nil, fmt.Errorf("pebble: open: %w", err)
	}
	s := &Store{db: db, wo: pdb.Sync, now: time.Now}
	if !cfg.SyncWrites {
		s.wo = pdb.NoSync
	}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

func (s *Store) Ping(context.Context) error { return nil }

func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: 1, MaxBatch: maxBatch, AtomicBatch: true, MultiProcess: false}
}

func (s *Store) Close() error {
	var err error
	s.once.Do(func() { err = s.db.Close() })
	return err
}

// get returns a copy of the value or (nil, false) when absent.
func (s *Store) get(key []byte) ([]byte, bool, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pdb.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := append([]byte{}, v...)
	_ = closer.Close()
	return out, true, nil
}

// counter helpers operate inside a batch: read current from the db (the batch
// is applied atomically, and the mutex guarantees no concurrent writer).
func (s *Store) addCounter(b *pdb.Batch, key []byte, delta int64) error {
	cur := int64(0)
	if v, ok, err := s.get(key); err != nil {
		return err
	} else if ok {
		cur = int64(binary.LittleEndian.Uint64(v)) //nolint:gosec // stored as int64
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(cur+delta)) //nolint:gosec // stored as int64
	return b.Set(key, buf[:], nil)
}

func (s *Store) readCounter(key []byte) (int64, error) {
	v, ok, err := s.get(key)
	if err != nil || !ok {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(v)), nil //nolint:gosec // stored as int64
}

// commit applies a batch with the configured durability.
func (s *Store) commit(b *pdb.Batch) error {
	if err := b.Commit(s.wo); err != nil {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	return nil
}

// iterate walks [prefix, prefixEnd) and calls fn until it returns false.
func (s *Store) iterate(lower, upper []byte, fn func(k, v []byte) (bool, error)) error {
	it, err := s.db.NewIter(&pdb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		cont, err := fn(it.Key(), it.Value())
		if err != nil || !cont {
			return err
		}
	}
	return it.Error()
}

// setRow writes a row and adjusts index keys and counters from prev (nil = new row).
func (s *Store) setRow(b *pdb.Batch, prev *store.Row, next *store.Row) error {
	if prev != nil {
		switch prev.State {
		case store.StateReserved:
			if err := b.Delete(expiryKey(prev.Pool, prev.ExpiresAt, prev.Outpoint), nil); err != nil {
				return err
			}
		case store.StateQuarantined:
			if err := b.Delete(quarantineKey(prev.Pool, prev.QuarantinedAt, prev.Outpoint), nil); err != nil {
				return err
			}
		}
		if prev.State != next.State {
			if err := s.addCounter(b, rowCounterKey(prev.Pool, prev.State), -1); err != nil {
				return err
			}
		}
	}
	if prev == nil || prev.State != next.State {
		if err := s.addCounter(b, rowCounterKey(next.Pool, next.State), 1); err != nil {
			return err
		}
	}
	switch next.State {
	case store.StateReserved:
		if err := b.Set(expiryKey(next.Pool, next.ExpiresAt, next.Outpoint), nil, nil); err != nil {
			return err
		}
	case store.StateQuarantined:
		if err := b.Set(quarantineKey(next.Pool, next.QuarantinedAt, next.Outpoint), nil, nil); err != nil {
			return err
		}
	}
	return b.Set(rowKey(next.Outpoint), encodeRow(next), nil)
}

// setShard writes a shard and adjusts the adoption index and counters.
func (s *Store) setShard(b *pdb.Batch, prev *store.Shard, next *store.Shard) error {
	if prev != nil {
		if err := b.Delete(adoptKey(prev.Pool, prev.Status, prev.CreatedAt, prev.TxID), nil); err != nil {
			return err
		}
		if prev.Status != next.Status {
			if err := s.addCounter(b, shardCounterKey(prev.Pool, prev.Status), -1); err != nil {
				return err
			}
		}
	}
	if prev == nil || prev.Status != next.Status {
		if err := s.addCounter(b, shardCounterKey(next.Pool, next.Status), 1); err != nil {
			return err
		}
	}
	if err := b.Set(adoptKey(next.Pool, next.Status, next.CreatedAt, next.TxID), nil, nil); err != nil {
		return err
	}
	return b.Set(shardKey(next.Pool, next.TxID), encodeShard(next), nil)
}

func (s *Store) loadRow(op store.Outpoint) (*store.Row, error) {
	v, ok, err := s.get(rowKey(op))
	if err != nil || !ok {
		return nil, err
	}
	r, err := decodeRow(v)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Store) loadShard(pool string, txid [32]byte) (*store.Shard, error) {
	v, ok, err := s.get(shardKey(pool, txid))
	if err != nil || !ok {
		return nil, err
	}
	sh, err := decodeShard(v)
	if err != nil {
		return nil, err
	}
	return &sh, nil
}
```
`addCounter` reads the committed value, so two counter updates for the same key inside ONE batch would clobber each other. Every method below therefore accumulates counter deltas in a `map[string]int64` and applies them once at the end with `applyCounters(b, deltas)`. Implement `setRow`/`setShard` to take a `deltas map[string]int64` parameter instead of calling `addCounter`, and add:
```go
func (s *Store) applyCounters(b *pdb.Batch, deltas map[string]int64) error {
	for k, d := range deltas {
		if err := s.addCounter(b, []byte(k), d); err != nil {
			return err
		}
	}
	return nil
}
```
(`deltas[string(rowCounterKey(pool, st))] += n`).

- [ ] **Step 6: Write `pkg/store/pebble/shards.go`** — method predicates:

- `PutShard`: validate; lock; if `loadShard` finds it → compare size/denom/mode → `ErrShardExists` or nil; else one batch: `setRow(nil, row)` for each row whose key does not already exist (available, cleared reservation fields, `UpdatedAt=now`), then `setShard(nil, shard{Owner:"", Version:1, CreatedAt:now, UpdatedAt:now})`; `applyCounters`; commit.
- `GetShard`: `loadShard` → `ErrNotFound` if nil.
- `UpdateShard`: validate status; lock; load; nil → `ErrNotFound`; terminal → `ErrTerminalShard`; copy, set status, BEEF if non-nil, `Version++`, `UpdatedAt`; `setShard(prev, next)`; commit.
- `RetireShard`: lock; load; nil → `ErrNotFound`; next = invalid, owner "", lease zero, `Version++`; iterate `rowPrefix(txid)` rows with `Pool==pool` and non-terminal state → `setRow(prev, next{State: invalid, UpdatedAt})`; count; commit.
- `AcquireShard`: validate; lock; for each status in `statuses` iterate `adoptPrefix(pool, status)` keys in order (created, txid): decode txid from the key tail, skip excluded, `loadShard`, skip if `Owner != "" && !LeaseUntil.IsZero() && !LeaseUntil.Before(now)`; keep the candidate with the smallest `(CreatedAt, TxID)` across statuses; if none → `(nil, nil)`; set Owner/LeaseUntil/UpdatedAt; `setShard(prev, next)`; commit; return copy.
- `RenewShard`: validate holder; lock; load; nil → `ErrNotFound`; `Owner != holder` → `ErrNotOwner`; set LeaseUntil; write (setShard with same status); return head.
- `ReleaseShard`: lock; load; nil → `ErrNotFound`; if `Owner == holder` clear Owner/LeaseUntil, UpdatedAt, write.
- `ListShards`: iterate `shardPrefix(pool)`, decode (BEEF dropped), filter statuses/owner, sort by `(CreatedAt, TxID)`, apply limit.

- [ ] **Step 7: Write `pkg/store/pebble/rows.go`** — `Reserve` and `Quarantine` in full; the rest by predicate:

```go
func (s *Store) Reserve(_ context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	if err := store.ValidateReserveOps(ops, maxBatch); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	b := s.db.NewBatch()
	defer b.Close()
	deltas := map[string]int64{}
	applied := make([]store.ReserveOp, 0, len(ops))
	for _, op := range ops {
		r, err := s.loadRow(op.Outpoint)
		if err != nil {
			return nil, wrapErr(err)
		}
		if r == nil || r.Pool != pool {
			continue
		}
		switch {
		case r.State == store.StateAvailable:
			prev := *r
			r.State, r.ReservedBy, r.ReservedAt, r.ExpiresAt, r.UpdatedAt = store.StateReserved, op.Token, now, op.ExpiresAt, now
			if err := s.setRow(b, &prev, r, deltas); err != nil {
				return nil, wrapErr(err)
			}
			applied = append(applied, op)
		case r.State == store.StateReserved && r.ReservedBy == op.Token:
			op.ExpiresAt = r.ExpiresAt
			applied = append(applied, op)
		}
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return nil, wrapErr(err)
	}
	if err := s.commit(b); err != nil {
		return nil, err
	}
	return applied, nil
}

func (s *Store) Quarantine(_ context.Context, pool string, before time.Time, limit int) (int, error) {
	if err := store.ValidateLimit(limit); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	prefix := expiryPrefix(pool)
	var due []store.Outpoint
	err := s.iterate(prefix, upperBoundBefore(prefix, before), func(k, _ []byte) (bool, error) {
		tail := k[len(prefix)+8:]
		var op store.Outpoint
		copy(op.TxID[:], tail[:32])
		op.Vout = binary.BigEndian.Uint32(tail[32:36])
		due = append(due, op)
		return len(due) < limit, nil
	})
	if err != nil {
		return 0, wrapErr(err)
	}
	b := s.db.NewBatch()
	defer b.Close()
	deltas := map[string]int64{}
	n := 0
	for _, op := range due {
		r, err := s.loadRow(op)
		if err != nil {
			return 0, wrapErr(err)
		}
		if r == nil || r.State != store.StateReserved || !r.ExpiresAt.Before(before) {
			continue
		}
		prev := *r
		r.State, r.QuarantinedAt, r.UpdatedAt = store.StateQuarantined, now, now
		if err := s.setRow(b, &prev, r, deltas); err != nil {
			return 0, wrapErr(err)
		}
		n++
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return 0, wrapErr(err)
	}
	if err := s.commit(b); err != nil {
		return 0, err
	}
	return n, nil
}
```
with `func wrapErr(err error) error { if err == nil { return nil }; return fmt.Errorf("%w: %w", store.ErrUnavailable, err) }` in `pebble.go`.

Remaining row methods:
- `ListAvailable`: iterate `rowPrefix(txid)` to `prefixEnd`; decode; keep `Pool==pool && State==available && Vout>=fromVout`; rows are already in vout order (key order); stop at limit.
- `GetRows`: validate; `loadRow` each; keep `Pool==pool`.
- `Release` / `Spend` / `Resolve`: lock; one batch; per op `loadRow`, check predicate (`reserved && ReservedBy==token` for Release/Spend; `quarantined` for Resolve), mutate exactly as the memory store does (clear fields on Release/Resolve-available; set hint+QuarantinedAt or SpentBy on Spend; SpentBy on Resolve-spent), `setRow(prev, next, deltas)`, count; apply counters; commit.
- `ListQuarantined`: iterate `quarantinePrefix(pool)` to `upperBoundBefore(prefix, before)`, decode outpoints from key tails, `loadRow`, keep `State==quarantined`; limit.
- `Stats`: read the ten counters.
- Leases (`lease.go`): lock; `get(leaseKey)`; decode; grant/renew/lose exactly as the memory store; write JSON via a batch (or `db.Set` with `s.wo`).

- [ ] **Step 8: Run the suite, lint, commit**

Run: `cd /git/fuelkeeper && go test -race -count=1 ./pkg/store/pebble/ && FUELKEEPER_STRESS=3 go test -race -count=1 -run 'TestConformance/(ReserveExclusivity|ReserveReleaseCycle|AcquireShardExclusive|ReserveVsQuarantine)' ./pkg/store/pebble/ && golangci-lint run ./pkg/store/pebble/`
Expected: PASS. Typical first failures: counters drifting (a state change not routed through `setRow`), index keys left behind after a transition (compare `TestConformance/QuarantineByExpiry` and `ListAvailablePagination`), or `ListShards` ordering (sort explicitly).

```bash
git add pkg/store/pebble go.mod go.sum
git commit -m "feat(store): pebble backend with indexed keys, counters and single-writer batches"
```

### Task 2: MongoDB backend (`pkg/store/mongodb`)

Multi-process, no multi-document transactions. Every mutation is a per-document conditional update; batches are classified by a follow-up read. Write concern `majority` with journaling; read preference primary. No TTL index on rows, ever.

**Collections and documents** (unix-microsecond `int64` times; `[]byte` fields encode as BSON binary):

| collection | `_id` | fields |
|---|---|---|
| `fuel_rows` | `txid(32) ‖ vout(4 BE)` as bytes | `p` pool, `sh` txid bytes, `v` vout, `sat`, `ls`, `us`, `st` state, `rb` token, `ra`, `ea`, `qa`, `h` hint, `sb` spent_by, `ua` |
| `fuel_shards` | `pool + "|" + txid hex` | `p`, `t` txid bytes, `n` size, `d` denomination, `m` lock mode, `s` status, `o` owner, `lu` lease_until, `ver`, `b` BEEF, `ca`, `ua` |
| `fuel_leases` | name | `h` holder, `e` expires |

Indexes: rows `{p:1, sh:1, _id:1}` partial `{st:0}` (ListAvailable, sorted by `_id` = vout order within a shard), `{p:1, ea:1}` partial `{st:1}` (Quarantine), `{p:1, qa:1}` partial `{st:2}` (ListQuarantined), `{p:1, st:1}` (Stats, RetireShard); shards `{p:1, s:1, ca:1, t:1}` (AcquireShard, ListShards).

**Files:**
- Create: `pkg/store/mongodb/mongodb.go`, `docs.go`, `shards.go`, `rows.go`, `lease.go`
- Test: `pkg/store/mongodb/mongodb_test.go` (build tag `mongodb`), `pkg/store/mongodb/indexes_test.go`
- Modify: `deploy/docker-compose.yaml` (mongo service), `Makefile` (`test-integration` runs mongodb too)

- [ ] **Step 1: Compose + Makefile**

Add to `deploy/docker-compose.yaml`:
```yaml
  mongo:
    image: mongo:7
    ports:
      - "27017:27017"
    healthcheck:
      test: ["CMD", "mongosh", "--quiet", "--eval", "db.adminCommand('ping').ok"]
      interval: 2s
      timeout: 2s
      retries: 30
```
Makefile: add `MONGO_URI ?= mongodb://localhost:27017` and extend `test-integration`:
```make
test-integration:
	FUELKEEPER_TEST_POSTGRES_DSN="$(PG_DSN)" $(GO) test -race -count=1 -tags=postgres ./pkg/store/postgres/...
	FUELKEEPER_TEST_MONGODB_URI="$(MONGO_URI)" $(GO) test -race -count=1 -tags=mongodb ./pkg/store/mongodb/...
```

- [ ] **Step 2: Write the test harness `pkg/store/mongodb/mongodb_test.go`**

```go
//go:build mongodb

package mongodb_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/mongodb"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func testURI(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("FUELKEEPER_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("FUELKEEPER_TEST_MONGODB_URI not set")
	}
	return uri
}

// isolatedStore opens a store on a fresh random database and drops it on cleanup.
func isolatedStore(t *testing.T, uri string, now func() time.Time) *mongodb.Store {
	t.Helper()
	var b [6]byte
	_, _ = rand.Read(b[:])
	db := "fk_" + hex.EncodeToString(b[:])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := mongodb.New(ctx, mongodb.Config{URI: uri, Database: db}, mongodb.WithClock(now))
	require.NoError(t, err)
	t.Cleanup(func() {
		c, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err == nil {
			_ = c.Database(db).Drop(context.Background())
			_ = c.Disconnect(context.Background())
		}
	})
	return s
}

func TestConformance(t *testing.T) {
	uri := testURI(t)
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		return isolatedStore(t, uri, now)
	})
}
```

`pkg/store/mongodb/indexes_test.go`:
```go
//go:build mongodb

package mongodb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestIndexesAndNoTTL pins the partial indexes and asserts no TTL index exists on rows
// (expiry is a CAS transition, never a delete).
func TestIndexesAndNoTTL(t *testing.T) {
	uri := testURI(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, uri, clk.Now)
	defer s.Close()
	cur, err := s.Rows().Indexes().List(context.Background())
	require.NoError(t, err)
	var specs []bson.M
	require.NoError(t, cur.All(context.Background(), &specs))
	names := map[string]bson.M{}
	for _, sp := range specs {
		names[sp["name"].(string)] = sp
		_, ttl := sp["expireAfterSeconds"]
		require.False(t, ttl, "TTL index %v must not exist", sp["name"])
	}
	for _, want := range []string{"rows_avail", "rows_expiry", "rows_quar", "rows_pool_state"} {
		require.Contains(t, names, want)
	}
	require.NotNil(t, names["rows_avail"]["partialFilterExpression"])
	require.NotNil(t, names["rows_expiry"]["partialFilterExpression"])
	require.NotNil(t, names["rows_quar"]["partialFilterExpression"])
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cd /git/fuelkeeper && go get go.mongodb.org/mongo-driver/v2@v2.9.1 && make docker-up && FUELKEEPER_TEST_MONGODB_URI=mongodb://localhost:27017 go test -tags=mongodb ./pkg/store/mongodb/`
Expected: build FAIL.

- [ ] **Step 4: Write `pkg/store/mongodb/docs.go`**

```go
package mongodb

import (
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

type rowDoc struct {
	ID    []byte `bson:"_id"`
	Pool  string `bson:"p"`
	Shard []byte `bson:"sh"`
	Vout  int64  `bson:"v"`
	Sats  int64  `bson:"sat"`
	Lock  []byte `bson:"ls"`
	Unl   []byte `bson:"us"`
	State int32  `bson:"st"`
	RB    []byte `bson:"rb,omitempty"`
	RA    int64  `bson:"ra,omitempty"`
	EA    int64  `bson:"ea,omitempty"`
	QA    int64  `bson:"qa,omitempty"`
	Hint  []byte `bson:"h,omitempty"`
	SB    []byte `bson:"sb,omitempty"`
	UA    int64  `bson:"ua"`
}

type shardDoc struct {
	ID      string `bson:"_id"`
	Pool    string `bson:"p"`
	TxID    []byte `bson:"t"`
	Size    int32  `bson:"n"`
	Denom   int64  `bson:"d"`
	Mode    int32  `bson:"m"`
	Status  int32  `bson:"s"`
	Owner   string `bson:"o"`
	LU      int64  `bson:"lu,omitempty"`
	Version int64  `bson:"ver"`
	BEEF    []byte `bson:"b"`
	CA      int64  `bson:"ca"`
	UA      int64  `bson:"ua"`
}

type leaseDoc struct {
	ID      string `bson:"_id"`
	Holder  string `bson:"h"`
	Expires int64  `bson:"e"`
}

func us(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMicro()
}

func fromUS(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMicro(v).UTC()
}

func rowID(op store.Outpoint) []byte {
	id := make([]byte, 0, 36)
	id = append(id, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(id, op.Vout)
}

func shardID(pool string, txid chainhash.Hash) string { return pool + "|" + txid.String() }

func toRow(d rowDoc) store.Row {
	var r store.Row
	copy(r.TxID[:], d.ID[:32])
	r.Vout = binary.BigEndian.Uint32(d.ID[32:36])
	r.Pool, r.Satoshis, r.LockingScript, r.UnlockingScript = d.Pool, uint64(d.Sats), d.Lock, d.Unl //nolint:gosec // stored from uint64
	r.State = store.State(d.State) //nolint:gosec // small enum
	copy(r.ReservedBy[:], d.RB)
	r.ReservedAt, r.ExpiresAt, r.QuarantinedAt, r.UpdatedAt = fromUS(d.RA), fromUS(d.EA), fromUS(d.QA), fromUS(d.UA)
	if len(d.Hint) == 32 {
		h, _ := chainhash.NewHash(d.Hint)
		r.SpentHint = h
	}
	if len(d.SB) == 32 {
		h, _ := chainhash.NewHash(d.SB)
		r.SpentBy = h
	}
	return r
}

func toShard(d shardDoc, withBEEF bool) store.Shard {
	var s store.Shard
	s.Pool, s.Size, s.Denomination, s.Owner, s.Version = d.Pool, int(d.Size), uint64(d.Denom), d.Owner, d.Version //nolint:gosec // stored from uint64
	copy(s.TxID[:], d.TxID)
	s.LockMode, s.Status = store.LockMode(d.Mode), store.ShardStatus(d.Status) //nolint:gosec // small enums
	s.LeaseUntil, s.CreatedAt, s.UpdatedAt = fromUS(d.LU), fromUS(d.CA), fromUS(d.UA)
	if withBEEF {
		s.BEEF = d.BEEF
	}
	return s
}
```

- [ ] **Step 5: Write `pkg/store/mongodb/mongodb.go`**

```go
// Package mongodb implements store.Store on MongoDB (driver v2). No multi-document
// transactions are used, so a standalone mongod works; every write is a
// conditional single-document update with write concern majority+journal.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

const maxBatch = 1000

// Config configures New.
type Config struct {
	URI         string
	Database    string
	MaxPoolSize uint64        // 0 = 64
	OpTimeout   time.Duration // per operation; 0 = 5s
}

// Option configures New.
type Option func(*Store)

// WithClock injects the clock.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// Store is a MongoDB store.Store.
type Store struct {
	client *mongo.Client
	db     *mongo.Database
	rows   *mongo.Collection
	shards *mongo.Collection
	leases *mongo.Collection
	now    func() time.Time
	opTO   time.Duration
}

var _ store.Store = (*Store)(nil)

// New connects, pings the primary and ensures indexes.
func New(ctx context.Context, cfg Config, opts ...Option) (*Store, error) {
	if cfg.URI == "" || cfg.Database == "" {
		return nil, fmt.Errorf("%w: mongodb uri and database are required", store.ErrInvalidArgument)
	}
	if cfg.MaxPoolSize == 0 {
		cfg.MaxPoolSize = 64
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = 5 * time.Second
	}
	j := true
	wc := &writeconcern.WriteConcern{W: "majority", Journal: &j}
	co := options.Client().ApplyURI(cfg.URI).SetMaxPoolSize(cfg.MaxPoolSize).SetWriteConcern(wc).SetReadPreference(readpref.Primary())
	client, err := mongo.Connect(co)
	if err != nil {
		return nil, fmt.Errorf("mongodb: connect: %w", err)
	}
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("mongodb: ping: %w", err)
	}
	db := client.Database(cfg.Database)
	s := &Store{client: client, db: db, rows: db.Collection("fuel_rows"), shards: db.Collection("fuel_shards"), leases: db.Collection("fuel_leases"), now: time.Now, opTO: cfg.OpTimeout}
	for _, o := range opts {
		o(s)
	}
	if err := s.ensureIndexes(ctx); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	return s, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	partial := func(state int) *options.IndexOptionsBuilder {
		return options.Index().SetPartialFilterExpression(bson.D{{Key: "st", Value: state}})
	}
	_, err := s.rows.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "p", Value: 1}, {Key: "sh", Value: 1}, {Key: "_id", Value: 1}}, Options: partial(0).SetName("rows_avail")},
		{Keys: bson.D{{Key: "p", Value: 1}, {Key: "ea", Value: 1}}, Options: partial(1).SetName("rows_expiry")},
		{Keys: bson.D{{Key: "p", Value: 1}, {Key: "qa", Value: 1}}, Options: partial(2).SetName("rows_quar")},
		{Keys: bson.D{{Key: "p", Value: 1}, {Key: "st", Value: 1}}, Options: options.Index().SetName("rows_pool_state")},
	})
	if err != nil {
		return fmt.Errorf("mongodb: row indexes: %w", err)
	}
	_, err = s.shards.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "p", Value: 1}, {Key: "s", Value: 1}, {Key: "ca", Value: 1}, {Key: "t", Value: 1}}, Options: options.Index().SetName("shards_adopt")},
	})
	if err != nil {
		return fmt.Errorf("mongodb: shard indexes: %w", err)
	}
	return nil
}

// Rows exposes the rows collection (tests).
func (s *Store) Rows() *mongo.Collection { return s.rows }

func (s *Store) Ping(ctx context.Context) error { return wrap(s.client.Ping(ctx, readpref.Primary())) }

func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: 8, MaxBatch: maxBatch, AtomicBatch: false, MultiProcess: true}
}

func (s *Store) Close() error {
	if s.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.client.Disconnect(ctx)
	s.client = nil
	return err
}

func (s *Store) op(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.opTO)
}

// wrap maps network/timeout errors to store.ErrUnavailable.
func wrap(err error) error {
	if err == nil {
		return nil
	}
	if mongo.IsTimeout(err) || mongo.IsNetworkError(err) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	return err
}
```

- [ ] **Step 6: Write `pkg/store/mongodb/shards.go`** — `AcquireShard` in full, the rest by predicate:

```go
func (s *Store) AcquireShard(ctx context.Context, pool, holder string, now, until time.Time, statuses []store.ShardStatus, exclude []chainhash.Hash) (*store.Shard, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return nil, err
	}
	if err := store.ValidateShardStatuses(statuses); err != nil {
		return nil, err
	}
	sts := make(bson.A, len(statuses))
	for i, st := range statuses {
		sts[i] = int32(st)
	}
	ex := make(bson.A, len(exclude))
	for i := range exclude {
		ex[i] = exclude[i][:]
	}
	filter := bson.D{
		{Key: "p", Value: pool},
		{Key: "s", Value: bson.D{{Key: "$in", Value: sts}}},
		{Key: "t", Value: bson.D{{Key: "$nin", Value: ex}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "o", Value: ""}},
			bson.D{{Key: "lu", Value: bson.D{{Key: "$exists", Value: false}}}},
			bson.D{{Key: "lu", Value: bson.D{{Key: "$lt", Value: us(now)}}}},
		}},
	}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "o", Value: holder}, {Key: "lu", Value: us(until)}, {Key: "ua", Value: us(now)}}}}
	octx, cancel := s.op(ctx)
	defer cancel()
	var d shardDoc
	err := s.shards.FindOneAndUpdate(octx, filter, update,
		options.FindOneAndUpdate().SetSort(bson.D{{Key: "ca", Value: 1}, {Key: "t", Value: 1}}).SetReturnDocument(options.After)).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil //nolint:nilnil // contract: nothing adoptable
	}
	if err != nil {
		return nil, wrap(err)
	}
	sh := toShard(d, true)
	return &sh, nil
}
```
Other methods:
- `PutShard`: validate; `FindOne` shard by `_id` → exists → compare `n/d/m` → `ErrShardExists` or nil. Else `InsertMany` rows (`SetOrdered(false)`; ignore `mongo.IsDuplicateKeyError` / `BulkWriteException` consisting only of dup-key errors), then `InsertOne` shard (`o:""`, `ver:1`, `ca=ua=now`); a dup-key on the shard insert means a concurrent identical PutShard won: return nil. Rows first, shard last.
- `GetShard`: `FindOne` by `_id`; `ErrNoDocuments` → `ErrNotFound`.
- `UpdateShard`: `UpdateOne({_id, s: {$in:[0,1]}}, $set {s, b (only if beef != nil), ua}, $inc {ver:1})`; `MatchedCount==0` → `FindOne` → missing → `ErrNotFound`, else `ErrTerminalShard`.
- `RetireShard`: `UpdateOne({_id}, $set {s:3, o:"", ua}, $unset {lu}, $inc {ver:1})` → `MatchedCount==0` → `ErrNotFound`; then `UpdateMany({p, sh: txid, st: {$in:[0,1,2]}}, $set {st:4, ua})` → `ModifiedCount`.
- `RenewShard`: `FindOneAndUpdate({_id, o: holder}, $set {lu}, ReturnDocument After)` → `ErrNoDocuments` → `FindOne({_id})` → missing → `ErrNotFound` else `ErrNotOwner`; return `ShardHead{s, ver}`.
- `ReleaseShard`: `UpdateOne({_id, o: holder}, $set {o:"", ua}, $unset {lu})`; if `MatchedCount==0` check existence → `ErrNotFound` if missing else nil.
- `ListShards`: `Find({p, s: {$in} (if set), o (if set)})` sort `{ca:1, t:1}` limit; projection excludes `b`.

- [ ] **Step 7: Write `pkg/store/mongodb/rows.go`** — `Reserve` and `Quarantine` in full:

```go
func (s *Store) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	if err := store.ValidateReserveOps(ops, maxBatch); err != nil {
		return nil, err
	}
	now := us(s.now())
	models := make([]mongo.WriteModel, len(ops))
	ids := make(bson.A, len(ops))
	byID := make(map[string]int, len(ops))
	for i, op := range ops {
		id := rowID(op.Outpoint)
		ids[i] = id
		byID[string(id)] = i
		tok := op.Token[:]
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{
				{Key: "_id", Value: id}, {Key: "p", Value: pool},
				{Key: "$or", Value: bson.A{bson.D{{Key: "st", Value: 0}}, bson.D{{Key: "st", Value: 1}, {Key: "rb", Value: tok}}}},
			}).
			SetUpdate(bson.A{bson.D{{Key: "$set", Value: bson.D{
				{Key: "ra", Value: bson.D{{Key: "$cond", Value: bson.A{bson.D{{Key: "$eq", Value: bson.A{"$st", 0}}}, now, "$ra"}}}},
				{Key: "ea", Value: bson.D{{Key: "$cond", Value: bson.A{bson.D{{Key: "$eq", Value: bson.A{"$st", 0}}}, us(op.ExpiresAt), "$ea"}}}},
				{Key: "st", Value: 1}, {Key: "rb", Value: tok}, {Key: "ua", Value: now},
			}}}})
	}
	octx, cancel := s.op(ctx)
	defer cancel()
	if _, err := s.rows.BulkWrite(octx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		var bwe mongo.BulkWriteException
		if !errors.As(err, &bwe) {
			return nil, wrap(err)
		}
		// per-document write errors are not expected for updates; treat as unavailable
		return nil, fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	// Classify by reading back: applied == reserved under this op's token.
	cur, err := s.rows.Find(octx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}, {Key: "p", Value: pool}, {Key: "st", Value: 1}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}, {Key: "rb", Value: 1}, {Key: "ea", Value: 1}}))
	if err != nil {
		return nil, wrap(err)
	}
	var docs []struct {
		ID []byte `bson:"_id"`
		RB []byte `bson:"rb"`
		EA int64  `bson:"ea"`
	}
	if err := cur.All(octx, &docs); err != nil {
		return nil, wrap(err)
	}
	applied := make([]store.ReserveOp, 0, len(ops))
	for _, d := range docs {
		i, ok := byID[string(d.ID)]
		if !ok || !bytes.Equal(d.RB, ops[i].Token[:]) {
			continue
		}
		op := ops[i]
		op.ExpiresAt = fromUS(d.EA)
		applied = append(applied, op)
	}
	return applied, nil
}

func (s *Store) Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error) {
	if err := store.ValidateLimit(limit); err != nil {
		return 0, err
	}
	octx, cancel := s.op(ctx)
	defer cancel()
	cur, err := s.rows.Find(octx, bson.D{{Key: "p", Value: pool}, {Key: "st", Value: 1}, {Key: "ea", Value: bson.D{{Key: "$lt", Value: us(before)}}}},
		options.Find().SetSort(bson.D{{Key: "ea", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(int64(limit)).SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return 0, wrap(err)
	}
	var ids []struct {
		ID []byte `bson:"_id"`
	}
	if err := cur.All(octx, &ids); err != nil {
		return 0, wrap(err)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	now := us(s.now())
	models := make([]mongo.WriteModel, len(ids))
	for i, d := range ids {
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: d.ID}, {Key: "st", Value: 1}, {Key: "ea", Value: bson.D{{Key: "$lt", Value: us(before)}}}}).
			SetUpdate(bson.D{{Key: "$set", Value: bson.D{{Key: "st", Value: 2}, {Key: "qa", Value: now}, {Key: "ua", Value: now}}}})
	}
	res, err := s.rows.BulkWrite(octx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, wrap(err)
	}
	return int(res.ModifiedCount), nil
}
```
Other row methods:
- `ListAvailable`: `Find({p, sh: txid, st:0, _id: {$gte: rowID(txid, fromVout)}})` sort `_id:1` limit.
- `GetRows`: `Find({_id: {$in}, p})`.
- `Release`: `BulkWrite` of `UpdateOne({_id, p, st:1, rb: tok}, $set {st:0, ua}, $unset {rb, ra, ea, qa, h})` → `ModifiedCount`.
- `Spend`: terminal → `$set {st:3, sb: txid, ua}`; else `$set {st:2, qa: now, h: txid, ua}`; same filter; `ModifiedCount`.
- `ListQuarantined`: `Find({p, st:2, qa: {$lt: before}})` sort `{qa:1, _id:1}` limit.
- `Resolve`: filter `{_id, p, st:2}`; available → `$set {st:0, ua}, $unset {rb, ra, ea, qa, h}`; spent → `$set {st:3, sb, ua}`; `ModifiedCount`.
- `Stats`: `Aggregate` `[{$match:{p}}, {$group:{_id:"$st", n:{$sum:1}}}]` on rows and `[{$match:{p}}, {$group:{_id:"$s", n:{$sum:1}}}]` on shards.
- Leases (`lease.go`): `TryAcquireOrRenew`: `FindOneAndUpdate({_id: name, $or: [{h: holder}, {e: {$lte: now}}]}, $set {h, e: now+ttl}, upsert, ReturnDocument After)`; a duplicate-key error means another holder owns an unexpired lease → return zero time, nil (arcade's pattern). `ReleaseLease`: `DeleteOne({_id: name, h: holder})`.

- [ ] **Step 8: Run the suite, lint, commit**

Run: `cd /git/fuelkeeper && FUELKEEPER_TEST_MONGODB_URI=mongodb://localhost:27017 go test -race -count=1 -tags=mongodb ./pkg/store/mongodb/ && golangci-lint run --build-tags mongodb ./pkg/store/mongodb/`
Expected: PASS, including `TestIndexesAndNoTTL`.

```bash
git add pkg/store/mongodb deploy/docker-compose.yaml Makefile go.mod go.sum
git commit -m "feat(store): mongodb backend with conditional updates, partial indexes and majority writes"
```

### Task 3: Aerospike backend (`pkg/store/aerospike`)

Multi-process, per-record CAS via filter expressions, presence-encoded index bins (the claimKey pattern from go-arcade-toolbox's aerostore). Never generation checks (any writer bumps generation), never client auto-retries on non-idempotent writes.

**Sets and bins** (namespace from config; set names get a configurable prefix so tests can isolate):

| set | key | bins |
|---|---|---|
| `<prefix>rows` | 36 bytes `txid ‖ vout(BE)` | `pool`, `txid` (blob), `vout` (int), `sat`, `ls`, `us`, `st`, `rb` (blob), `ra`, `ea`, `qa` (int µs), `hint`, `sb`, `ua`, and index bins `ak` = `pool|txidhex` present iff `st==0`; `pst` = `pool|st` always; `ea` present iff `st==1`; `qa` present iff `st==2` |
| `<prefix>shards` | `pool|txidhex` | `pool`, `txid`, `size`, `denom`, `mode`, `status`, `owner`, `lu`, `ver`, `beef`, `ca`, `ua`, index bin `ps` = `pool|status` |
| `<prefix>leases` | name | `holder`, `exp` |

Secondary indexes: rows `ak` STRING (ListAvailable = one equality query), `pst` STRING (Stats counts, RetireShard), `ea` NUMERIC, `qa` NUMERIC; shards `ps` STRING. Because `ea`/`qa` bins are removed on transition, their indexes are naturally partial. Query results are unordered: sort client-side.

**Policies:** every write `CommitLevel=COMMIT_ALL`, `MaxRetries=0`, `TotalTimeout` from ctx (default 3 s); reads/queries `MaxRetries=1`. Production requires a strong-consistency namespace (document it); tests run against CE with `default-ttl 0`.

**Files:**
- Create: `pkg/store/aerospike/aerospike.go`, `bins.go`, `shards.go`, `rows.go`, `lease.go`
- Test: `pkg/store/aerospike/aerospike_test.go` (build tag `aerospike`), `pkg/store/aerospike/policy_test.go`
- Modify: `deploy/docker-compose.yaml`, `deploy/aerospike.conf`, `Makefile`

- [ ] **Step 1: Compose, conf, Makefile**

`deploy/aerospike.conf`:
```
service { proto-fd-max 15000 }
logging { console { context any info } }
network {
  service { address any; port 3000 }
  heartbeat { mode mesh; port 3002; interval 150; timeout 10 }
  fabric { port 3001 }
}
namespace test {
  replication-factor 1
  default-ttl 0
  nsup-period 0
  storage-engine memory { data-size 1G }
}
```
Compose:
```yaml
  aerospike:
    image: aerospike/aerospike-server:7.1.0.0
    ports:
      - "3000:3000"
    volumes:
      - ./aerospike.conf:/etc/aerospike/aerospike.conf:ro
    healthcheck:
      test: ["CMD", "asinfo", "-v", "status"]
      interval: 3s
      timeout: 3s
      retries: 30
```
Makefile: `AERO_HOST ?= localhost:3000`, add to `test-integration`:
```make
	FUELKEEPER_TEST_AEROSPIKE_HOST="$(AERO_HOST)" $(GO) test -race -count=1 -tags=aerospike ./pkg/store/aerospike/...
```

- [ ] **Step 2: Write the test harness `pkg/store/aerospike/aerospike_test.go`**

```go
//go:build aerospike

package aerospike_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	aerostore "github.com/bsv-blockchain/fuelkeeper/pkg/store/aerospike"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func testHost(t *testing.T) string {
	t.Helper()
	h := os.Getenv("FUELKEEPER_TEST_AEROSPIKE_HOST")
	if h == "" {
		t.Skip("FUELKEEPER_TEST_AEROSPIKE_HOST not set")
	}
	return h
}

// isolatedStore uses a random set prefix per subtest and truncates those sets on cleanup.
func isolatedStore(t *testing.T, host string, now func() time.Time) *aerostore.Store {
	t.Helper()
	var b [4]byte
	_, _ = rand.Read(b[:])
	prefix := "fk" + hex.EncodeToString(b[:]) + "_"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := aerostore.New(ctx, aerostore.Config{Hosts: []string{host}, Namespace: "test", SetPrefix: prefix}, aerostore.WithClock(now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Truncate(context.Background()) })
	return s
}

func TestConformance(t *testing.T) {
	host := testHost(t)
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		return isolatedStore(t, host, now)
	})
}
```

`pkg/store/aerospike/policy_test.go` (no build tag; pure policy construction):
```go
package aerospike

import (
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
)

// TestWritePoliciesNeverRetry pins the two properties G1 depends on: writes are
// never auto-retried (a retried CAS success reads as a loss) and are fully committed.
func TestWritePoliciesNeverRetry(t *testing.T) {
	s := &Store{}
	wp := s.writePolicy(nil)
	assert.Equal(t, 0, wp.MaxRetries)
	assert.Equal(t, as.COMMIT_ALL, wp.CommitLevel)
	assert.Equal(t, as.UPDATE_ONLY, wp.RecordExistsAction)
	bwp := s.batchWritePolicy(nil)
	assert.Equal(t, as.COMMIT_ALL, bwp.CommitLevel)
	assert.Equal(t, as.UPDATE_ONLY, bwp.RecordExistsAction)
	bp := s.batchPolicy(nil)
	assert.Equal(t, 0, bp.MaxRetries)
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cd /git/fuelkeeper && go get github.com/aerospike/aerospike-client-go/v8@latest && go test ./pkg/store/aerospike/`
Expected: build FAIL.

- [ ] **Step 4: Write `pkg/store/aerospike/aerospike.go` and `bins.go`**

`aerospike.go`:
```go
// Package aerospike implements store.Store on Aerospike using per-record filter
// expressions as the CAS primitive and presence-encoded index bins.
package aerospike

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

const maxBatch = 1000

// Config configures New.
type Config struct {
	Hosts     []string // host:port
	Namespace string
	SetPrefix string        // e.g. "fuel_" (production) or a random prefix (tests)
	Timeout   time.Duration // default per-op total timeout; 0 = 3s
	PoolSize  int           // connection queue size; 0 = 128
}

// Option configures New.
type Option func(*Store)

// WithClock injects the clock.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// Store is an Aerospike store.Store.
type Store struct {
	client    *as.Client
	ns        string
	rowsSet   string
	shardsSet string
	leasesSet string
	timeout   time.Duration
	now       func() time.Time
}

var _ store.Store = (*Store)(nil)

// New connects and ensures secondary indexes.
func New(ctx context.Context, cfg Config, opts ...Option) (*Store, error) {
	if len(cfg.Hosts) == 0 || cfg.Namespace == "" {
		return nil, fmt.Errorf("%w: aerospike hosts and namespace are required", store.ErrInvalidArgument)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 3 * time.Second
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 128
	}
	if cfg.SetPrefix == "" {
		cfg.SetPrefix = "fuel_"
	}
	hosts := make([]*as.Host, 0, len(cfg.Hosts))
	for _, h := range cfg.Hosts {
		host, port, err := splitHostPort(h)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, as.NewHost(host, port))
	}
	cp := as.NewClientPolicy()
	cp.ConnectionQueueSize = cfg.PoolSize
	cp.IdleTimeout = 55 * time.Second
	cp.Timeout = cfg.Timeout
	client, aerr := as.NewClientWithPolicyAndHost(cp, hosts...)
	if aerr != nil {
		return nil, fmt.Errorf("aerospike: connect: %w", aerr)
	}
	s := &Store{client: client, ns: cfg.Namespace, rowsSet: cfg.SetPrefix + "rows", shardsSet: cfg.SetPrefix + "shards", leasesSet: cfg.SetPrefix + "leases", timeout: cfg.Timeout, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	if err := s.ensureIndexes(ctx); err != nil {
		client.Close()
		return nil, err
	}
	return s, nil
}

func splitHostPort(h string) (string, int, error) {
	host, portStr, ok := strings.Cut(h, ":")
	if !ok {
		return h, 3000, nil
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		return "", 0, fmt.Errorf("%w: aerospike host %q", store.ErrInvalidArgument, h)
	}
	return host, port, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	type idx struct {
		set, bin string
		typ      as.IndexType
	}
	for _, ix := range []idx{
		{s.rowsSet, "ak", as.STRING}, {s.rowsSet, "pst", as.STRING}, {s.rowsSet, "ea", as.NUMERIC}, {s.rowsSet, "qa", as.NUMERIC},
		{s.shardsSet, "ps", as.STRING},
	} {
		name := ix.set + "_" + ix.bin
		task, aerr := s.client.CreateIndex(s.writePolicy(ctx), s.ns, ix.set, name, ix.bin, ix.typ)
		if aerr != nil {
			if aerr.Matches(types.INDEX_FOUND) {
				continue
			}
			return fmt.Errorf("aerospike: create index %s: %w", name, aerr)
		}
		if err := <-task.OnComplete(); err != nil {
			return fmt.Errorf("aerospike: index %s build: %w", name, err)
		}
	}
	return nil
}

// Truncate removes all records of this store's sets (tests).
func (s *Store) Truncate(context.Context) error {
	for _, set := range []string{s.rowsSet, s.shardsSet, s.leasesSet} {
		if aerr := s.client.Truncate(nil, s.ns, set, nil); aerr != nil {
			return aerr
		}
	}
	return nil
}

func (s *Store) Ping(context.Context) error {
	if !s.client.IsConnected() {
		return store.ErrUnavailable
	}
	return nil
}

func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: 8, MaxBatch: maxBatch, AtomicBatch: false, MultiProcess: true}
}

func (s *Store) Close() error {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
	return nil
}

func (s *Store) totalTimeout(ctx context.Context) time.Duration {
	if ctx == nil {
		return s.timeout
	}
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d > 0 && d < s.timeout {
			return d
		}
	}
	return s.timeout
}

func (s *Store) writePolicy(ctx context.Context) *as.WritePolicy {
	wp := as.NewWritePolicy(0, as.TTLDontExpire)
	wp.MaxRetries = 0
	wp.CommitLevel = as.COMMIT_ALL
	wp.RecordExistsAction = as.UPDATE_ONLY
	wp.TotalTimeout = s.totalTimeout(ctx)
	return wp
}

func (s *Store) batchWritePolicy(ctx context.Context) *as.BatchWritePolicy {
	bwp := as.NewBatchWritePolicy()
	bwp.CommitLevel = as.COMMIT_ALL
	bwp.RecordExistsAction = as.UPDATE_ONLY
	_ = ctx
	return bwp
}

func (s *Store) batchPolicy(ctx context.Context) *as.BatchPolicy {
	bp := as.NewBatchPolicy()
	bp.MaxRetries = 0
	bp.TotalTimeout = s.totalTimeout(ctx)
	return bp
}

func (s *Store) queryPolicy(ctx context.Context) *as.QueryPolicy {
	qp := as.NewQueryPolicy()
	qp.MaxRetries = 1
	qp.TotalTimeout = s.totalTimeout(ctx)
	return qp
}

func (s *Store) readPolicy(ctx context.Context) *as.BasePolicy {
	p := as.NewPolicy()
	p.MaxRetries = 1
	p.TotalTimeout = s.totalTimeout(ctx)
	return p
}

// wrap maps Aerospike errors: timeouts/connection → ErrUnavailable.
func wrap(aerr as.Error) error {
	if aerr == nil {
		return nil
	}
	if aerr.Matches(types.TIMEOUT, types.NETWORK_ERROR, types.NO_AVAILABLE_CONNECTIONS_TO_NODE, types.SERVER_NOT_AVAILABLE, types.INVALID_NODE_ERROR) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, aerr)
	}
	return aerr
}

var errFiltered = errors.New("filtered")
```
`bins.go`: record ↔ `store.Row`/`store.Shard` conversion (`rowBins(r) as.BinMap`, `rowFromRecord(rec) (store.Row, error)`, `shardBins`, `shardFromRecord`), key builders (`rowKey(op)`, `shardKey(pool, txid)` = `as.NewKey(ns, set, ...)`), index-bin helpers `akFor(pool, txid) string`, `pstFor(pool, st) string`, `psFor(pool, status) string`. Row bins for a new available row include `ak` and `pst`; `ea`/`qa` absent.

- [ ] **Step 5: Write `pkg/store/aerospike/rows.go`** — `Reserve` and `Quarantine` in full:

```go
func (s *Store) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	if err := store.ValidateReserveOps(ops, maxBatch); err != nil {
		return nil, err
	}
	now := us(s.now())
	recs := make([]as.BatchRecordIfc, len(ops))
	for i, op := range ops {
		bwp := s.batchWritePolicy(ctx)
		// CAS predicate: pool matches AND (available OR reserved under this token)
		bwp.FilterExpression = as.ExpAnd(
			as.ExpEq(as.ExpStringBin("pool"), as.ExpStringVal(pool)),
			as.ExpOr(
				as.ExpEq(as.ExpIntBin("st"), as.ExpIntVal(0)),
				as.ExpAnd(as.ExpEq(as.ExpIntBin("st"), as.ExpIntVal(1)), as.ExpEq(as.ExpBlobBin("rb"), as.ExpBlobVal(op.Token[:]))),
			),
		)
		wasAvailable := as.ExpEq(as.ExpIntBin("st"), as.ExpIntVal(0))
		recs[i] = as.NewBatchWrite(bwp, s.rowKey(op.Outpoint),
			// stamp ra/ea only on the available→reserved edge; keep them on a replay
			as.ExpWriteOp("ra", as.ExpCond(wasAvailable, as.ExpIntVal(now), as.ExpIntBin("ra")), as.ExpWriteFlagDefault),
			as.ExpWriteOp("ea", as.ExpCond(wasAvailable, as.ExpIntVal(us(op.ExpiresAt)), as.ExpIntBin("ea")), as.ExpWriteFlagDefault),
			as.PutOp(as.NewBin("st", 1)),
			as.PutOp(as.NewBin("rb", op.Token[:])),
			as.PutOp(as.NewBin("pst", pstFor(pool, store.StateReserved))),
			as.PutOp(as.NewBin("ak", nil)), // remove the claimable index bin
			as.PutOp(as.NewBin("ua", now)),
			as.GetBinOp("ea"),
		)
	}
	if aerr := s.client.BatchOperate(s.batchPolicy(ctx), recs); aerr != nil && !aerr.Matches(types.BATCH_FAILED) {
		return nil, wrap(aerr)
	}
	applied := make([]store.ReserveOp, 0, len(ops))
	for i, rec := range recs {
		br := rec.BatchRec()
		switch {
		case br.Err == nil:
			op := ops[i]
			if br.Record != nil && br.Record.Bins != nil {
				if v, ok := br.Record.Bins["ea"].(int); ok {
					op.ExpiresAt = fromUS(int64(v))
				}
			}
			applied = append(applied, op)
		case br.Err.Matches(types.FILTERED_OUT, types.KEY_NOT_FOUND_ERROR):
			// missed: lost the CAS or unknown outpoint
		default:
			if br.InDoubt || br.Err.Matches(types.TIMEOUT, types.NETWORK_ERROR) {
				return nil, fmt.Errorf("%w: %w", store.ErrUnavailable, br.Err) // whole batch retried idempotently by the caller
			}
			return nil, br.Err
		}
	}
	return applied, nil
}

func (s *Store) Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error) {
	if err := store.ValidateLimit(limit); err != nil {
		return 0, err
	}
	stmt := as.NewStatement(s.ns, s.rowsSet, "txid", "vout", "ea")
	if err := stmt.SetFilter(as.NewRangeFilter("ea", 0, us(before)-1)); err != nil {
		return 0, err
	}
	qp := s.queryPolicy(ctx)
	qp.FilterExpression = as.ExpAnd(as.ExpEq(as.ExpStringBin("pool"), as.ExpStringVal(pool)), as.ExpEq(as.ExpIntBin("st"), as.ExpIntVal(1)))
	rs, aerr := s.client.Query(qp, stmt)
	if aerr != nil {
		return 0, wrap(aerr)
	}
	type cand struct {
		op store.Outpoint
		ea int64
	}
	var cands []cand
	for res := range rs.Results() {
		if res.Err != nil {
			return 0, wrap(res.Err)
		}
		op, ok := outpointFromBins(res.Record.Bins)
		if !ok {
			continue
		}
		cands = append(cands, cand{op: op, ea: int64(res.Record.Bins["ea"].(int))})
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].ea != cands[j].ea {
			return cands[i].ea < cands[j].ea
		}
		return bytes.Compare(cands[i].op.TxID[:], cands[j].op.TxID[:]) < 0 || (cands[i].op.TxID == cands[j].op.TxID && cands[i].op.Vout < cands[j].op.Vout)
	})
	if len(cands) > limit {
		cands = cands[:limit]
	}
	if len(cands) == 0 {
		return 0, nil
	}
	now := us(s.now())
	recs := make([]as.BatchRecordIfc, len(cands))
	for i, c := range cands {
		bwp := s.batchWritePolicy(ctx)
		bwp.FilterExpression = as.ExpAnd(as.ExpEq(as.ExpIntBin("st"), as.ExpIntVal(1)), as.ExpLess(as.ExpIntBin("ea"), as.ExpIntVal(us(before))))
		recs[i] = as.NewBatchWrite(bwp, s.rowKey(c.op),
			as.PutOp(as.NewBin("st", 2)),
			as.PutOp(as.NewBin("qa", now)),
			as.PutOp(as.NewBin("ea", nil)),
			as.PutOp(as.NewBin("pst", pstFor(pool, store.StateQuarantined))),
			as.PutOp(as.NewBin("ua", now)),
		)
	}
	if aerr := s.client.BatchOperate(s.batchPolicy(ctx), recs); aerr != nil && !aerr.Matches(types.BATCH_FAILED) {
		return 0, wrap(aerr)
	}
	n := 0
	for _, rec := range recs {
		if rec.BatchRec().Err == nil {
			n++
		}
	}
	return n, nil
}
```
`as.PutOp(as.NewBin(name, nil))` deletes a bin. `outpointFromBins` reconstructs the outpoint from the `txid` blob and `vout` int bins.

Other row methods:
- `ListAvailable`: query `NewEqualFilter("ak", akFor(pool, txid))` with `FilterExpression vout >= fromVout`; collect, sort by vout, limit.
- `GetRows`: `BatchGet` of keys; keep records with `pool` bin equal.
- `Release` / `Spend` / `Resolve`: `BatchOperate` with per-record filter expression (`pool==X && st==1 && rb==token` for Release/Spend; `pool==X && st==2` for Resolve) and the bin puts/removals: Release/Resolve-available → `st=0`, `pst`, restore `ak`, remove `rb, ra, ea, qa, hint`; Spend non-terminal → `st=2, qa=now, hint, pst`, remove `ea`; Spend terminal / Resolve-spent → `st=3, sb, pst`, remove `ea`/`qa`. Count records with `Err == nil`.
- `ListQuarantined`: range query on `qa` `[0, before-1]` with `FilterExpression pool==X && st==2`; sort by `qa` then outpoint; limit.
- `Stats`: for each state, query `NewEqualFilter("pst", pstFor(pool, st))` with `qp.IncludeBinData=false` and count records; same for shards via `ps`. (Document: O(rows) per call; the janitor interval bounds it.)

- [ ] **Step 6: Write `pkg/store/aerospike/shards.go`** — `AcquireShard` in full:

```go
func (s *Store) AcquireShard(ctx context.Context, pool, holder string, now, until time.Time, statuses []store.ShardStatus, exclude []chainhash.Hash) (*store.Shard, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return nil, err
	}
	if err := store.ValidateShardStatuses(statuses); err != nil {
		return nil, err
	}
	skip := map[chainhash.Hash]bool{}
	for _, h := range exclude {
		skip[h] = true
	}
	free := as.ExpOr(as.ExpEq(as.ExpStringBin("owner"), as.ExpStringVal("")), as.ExpLess(as.ExpIntBin("lu"), as.ExpIntVal(us(now))))
	var cands []store.Shard
	for _, st := range statuses {
		stmt := as.NewStatement(s.ns, s.shardsSet)
		if err := stmt.SetFilter(as.NewEqualFilter("ps", psFor(pool, st))); err != nil {
			return nil, err
		}
		qp := s.queryPolicy(ctx)
		qp.FilterExpression = free
		rs, aerr := s.client.Query(qp, stmt)
		if aerr != nil {
			return nil, wrap(aerr)
		}
		for res := range rs.Results() {
			if res.Err != nil {
				return nil, wrap(res.Err)
			}
			sh, err := shardFromRecord(res.Record, false)
			if err != nil || skip[sh.TxID] {
				continue
			}
			cands = append(cands, sh)
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if !cands[i].CreatedAt.Equal(cands[j].CreatedAt) {
			return cands[i].CreatedAt.Before(cands[j].CreatedAt)
		}
		return bytes.Compare(cands[i].TxID[:], cands[j].TxID[:]) < 0
	})
	for _, c := range cands {
		wp := s.writePolicy(ctx)
		wp.FilterExpression = free
		rec, aerr := s.client.Operate(wp, s.shardKey(pool, c.TxID),
			as.PutOp(as.NewBin("owner", holder)), as.PutOp(as.NewBin("lu", us(until))), as.PutOp(as.NewBin("ua", us(now))),
			as.GetOp())
		if aerr != nil {
			if aerr.Matches(types.FILTERED_OUT) {
				continue // lost the race for this shard; try the next
			}
			return nil, wrap(aerr)
		}
		sh, err := shardFromRecord(rec, true)
		if err != nil {
			return nil, err
		}
		return &sh, nil
	}
	return nil, nil //nolint:nilnil // contract: nothing adoptable
}
```
The `lu` bin must always exist on a shard record (write `0` for "no lease") so `ExpLess(lu, now)` evaluates; unowned shards have `owner==""`.

Other shard methods:
- `PutShard`: `Get` shard key → exists → compare → `ErrShardExists`/nil; else `BatchOperate` of `NewBatchWrite` with `RecordExistsAction=CREATE_ONLY` for each row (ignore `KEY_EXISTS_ERROR` per record), then `Put` the shard with `CREATE_ONLY` (a `KEY_EXISTS_ERROR` here means a concurrent identical PutShard won → nil). Rows first, shard last.
- `GetShard`: `Get`; nil record → `ErrNotFound`.
- `UpdateShard`: `Operate` with `FilterExpression status in (0,1)`: puts `status`, `ps`, `beef` (if non-nil), `ua`, and `AddOp(ver, 1)`; `FILTERED_OUT` → `Get` to distinguish `ErrNotFound` / `ErrTerminalShard`.
- `RetireShard`: `Operate` shard (`status=3`, `ps`, `owner=""`, `lu=0`, `AddOp ver`), `KEY_NOT_FOUND` → `ErrNotFound`; then query rows by `ak` (available) and by `pst` for reserved/quarantined of this pool, keep `txid == shard`, `BatchOperate` with filter `st in (0,1,2)` → `st=4`, `pst`, remove `ak/ea/qa`; count successes.
- `RenewShard`: `Operate` with filter `owner == holder`: put `lu`, `GetOp()`; `FILTERED_OUT` → `Get` → missing → `ErrNotFound` else `ErrNotOwner`.
- `ReleaseShard`: `Operate` with filter `owner == holder`: `owner=""`, `lu=0`; `FILTERED_OUT` → check existence → `ErrNotFound` if missing else nil.
- `ListShards`: for each status (or all four), query `ps`; filter owner; sort; limit; BEEF dropped.
- Leases (`lease.go`): `TryAcquireOrRenew`: try `Put` with `CREATE_ONLY` (`holder`, `exp`); on `KEY_EXISTS_ERROR` do `Operate` with `FilterExpression holder==me || exp <= now`, `UPDATE_ONLY`: put `holder`, `exp`; `FILTERED_OUT` → zero time. `ReleaseLease`: `Delete` with `FilterExpression holder==me` (never an unguarded delete).

- [ ] **Step 7: Run the suite, lint, commit**

Run: `cd /git/fuelkeeper && make docker-up && FUELKEEPER_TEST_AEROSPIKE_HOST=localhost:3000 go test -race -count=1 -tags=aerospike ./pkg/store/aerospike/ && go test ./pkg/store/aerospike/ -run TestWritePolicies && golangci-lint run --build-tags aerospike ./pkg/store/aerospike/`
Expected: PASS. If `ExpWriteOp`/`ExpCond` names differ in the installed client version, look them up in `~/go/pkg/mod/github.com/aerospike/aerospike-client-go/v8@*/expression*.go` and `exp_ops.go`; the semantics (conditional write of `ra`/`ea` on the available→reserved edge) must be preserved. If expression write operations are unavailable on the server, fall back to two batch passes: first `GetBinOp("st")`, then write only the rows that were available with the stamps and the rest without — still under the same filter expression.

```bash
git add pkg/store/aerospike deploy Makefile go.mod go.sum
git commit -m "feat(store): aerospike backend with expression CAS, presence-encoded index bins and no-retry writes"
```

---

### Task 4: Factory entries, CI matrix, README matrix

**Files:**
- Modify: `pkg/store/factory/factory.go`, `pkg/store/factory/factory_test.go`, `.github/workflows/ci.yml`, `README.md`

- [ ] **Step 1: Factory**

Add `BackendPebble = "pebble"`, `BackendMongoDB = "mongodb"`, `BackendAerospike = "aerospike"` and config fields `Pebble pebble.Config \`yaml:"pebble"\``, `MongoDB mongodb.Config \`yaml:"mongodb"\``, `Aerospike aerospike.Config \`yaml:"aerospike"\``; switch cases calling `pebble.Open`, `mongodb.New`, `aerospike.New` with `WithClock(now)`; validation errors (`ErrInvalidArgument`) for missing `pebble.path`, `mongodb.uri/database`, `aerospike.hosts/namespace`. Extend `factory_test.go`: pebble opens on `t.TempDir()`; mongodb/aerospike without required fields → `ErrInvalidArgument`.

- [ ] **Step 2: CI**

In `.github/workflows/ci.yml` add a `mongo:7` service (port 27017) and set `FUELKEEPER_TEST_MONGODB_URI: mongodb://localhost:27017` on the `make test-integration` step; keep Aerospike out of GitHub CI (run locally via compose) and say so in the workflow comment. `make test` now covers memory, sqlite and pebble.

- [ ] **Step 3: README backend matrix** — replace the Plan 1 table:

```markdown
| Backend   | Multi-process | Conformance run          | Notes |
|-----------|---------------|--------------------------|-------|
| memory    | no            | `make test`              | tests and benchmarks |
| sqlite    | no            | `make test`              | single writer, `synchronous=FULL`, pragmas verified at open |
| pebble    | no            | `make test`              | embedded LSM, one batch per mutation, `SyncWrites` default on |
| postgres  | yes           | `make test-integration`  | `FOR UPDATE SKIP LOCKED` adoption, `unnest` CAS batches, partial indexes |
| mongodb   | yes           | `make test-integration`  | conditional single-document updates, write concern majority+journal, no TTL indexes |
| aerospike | yes           | `make test-integration` (local compose) | filter-expression CAS, presence-encoded index bins, `MaxRetries=0` on writes; needs a strong-consistency namespace in production |
```

- [ ] **Step 4: Verify and commit**

Run: `cd /git/fuelkeeper && make lint && make test && make test-integration`
Expected: all green with compose up.

```bash
git add pkg/store/factory .github/workflows/ci.yml README.md
git commit -m "feat(store): factory entries for pebble, mongodb and aerospike; CI matrix; backend docs"
```

---

## Self-review notes

- **Spec coverage:** Pebble key layout, batch atomicity, single write mutex, counters for Stats; Mongo `_id` layout, partial indexes, pipeline-update conditional stamps, follow-up read classification, majority+journal, no TTL index (pinned by test), `FindOneAndUpdate` adoption; Aerospike presence-encoded `ak`/`ea`/`qa`/`pst`/`ps` bins with secondary indexes, `BatchOperate` + filter-expression CAS, expression writes for conditional stamps, `MaxRetries=0` and `COMMIT_ALL` pinned by test, guarded deletes, strong-consistency note; factory, compose, CI, README.
- **Type consistency:** every backend exposes `Config`, `Option`, `WithClock`, `New`/`Open`, and satisfies `store.Store` (`var _ store.Store = (*Store)(nil)`); test harnesses follow the Plan 1 shape (`isolatedStore`, env-var skip, build tags).
- **Placeholders:** none; methods not shown in full are specified by predicate and reference implementation, and the suite pins them.

## Follow-on plans

5. `2026-09-29-05-treasury.md`
6. `2026-09-29-06-demo-benchmarks-docs.md`
