package pebble_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	pebblestore "github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// gateFS wraps a vfs.FS so a test can hold every WAL (*.log) sync until it
// says so, or make WAL syncs fail, to observe what the store does while a
// batch is applied but not yet durable.
type gateFS struct {
	vfs.FS
	mu      sync.Mutex
	gate    chan struct{} // non-nil: WAL syncs block until it is closed
	failErr error         // non-nil: WAL syncs fail with it
	entered chan struct{} // gets a value (non-blocking) when a sync blocks on gate
}

func newGateFS(fs vfs.FS) *gateFS { return &gateFS{FS: fs, entered: make(chan struct{}, 1)} }

func (g *gateFS) hold() {
	g.mu.Lock()
	g.gate = make(chan struct{})
	g.mu.Unlock()
}

func (g *gateFS) open() {
	g.mu.Lock()
	if g.gate != nil {
		close(g.gate)
		g.gate = nil
	}
	g.mu.Unlock()
}

func (g *gateFS) fail(err error) {
	g.mu.Lock()
	g.failErr = err
	g.mu.Unlock()
}

func (g *gateFS) beforeSync() error {
	g.mu.Lock()
	gate, ferr := g.gate, g.failErr
	g.mu.Unlock()
	if gate != nil {
		select {
		case g.entered <- struct{}{}:
		default:
		}
		<-gate
	}
	return ferr
}

func (g *gateFS) wrap(name string, f vfs.File, err error) (vfs.File, error) {
	if err != nil || !strings.HasSuffix(name, ".log") {
		return f, err
	}
	return &gateFile{File: f, fs: g}, nil
}

func (g *gateFS) Create(name string) (vfs.File, error) {
	f, err := g.FS.Create(name)
	return g.wrap(name, f, err)
}

func (g *gateFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	f, err := g.FS.ReuseForWrite(oldname, newname)
	return g.wrap(newname, f, err)
}

type gateFile struct {
	vfs.File
	fs *gateFS
}

func (f *gateFile) Sync() error {
	if err := f.fs.beforeSync(); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f *gateFile) SyncData() error {
	if err := f.fs.beforeSync(); err != nil {
		return err
	}
	return f.File.SyncData()
}

const durPath = "db"

// newStrictMem returns a crash-simulating MemFS holding an empty, durable
// durPath directory: on a real disk the data directory already exists, and
// Pebble syncs the entries inside it but never its parent, so without this
// the whole directory (not just unsynced data) would vanish in the "crash".
func newStrictMem(t *testing.T) *vfs.MemFS {
	t.Helper()
	mem := vfs.NewStrictMem()
	require.NoError(t, mem.MkdirAll(durPath, 0o755))
	root, err := mem.OpenDir("")
	require.NoError(t, err)
	require.NoError(t, root.Sync())
	require.NoError(t, root.Close())
	return mem
}

// crashAndReopen simulates a power loss on mem: syncs from here on are
// ignored, the store is closed, everything not synced before this call is
// discarded, and a fresh store is opened on what survived.
func crashAndReopen(t *testing.T, mem *vfs.MemFS, s *pebblestore.Store, beforeClose func()) *pebblestore.Store {
	t.Helper()
	mem.SetIgnoreSyncs(true)
	if beforeClose != nil {
		beforeClose()
	}
	_ = s.Close()
	mem.ResetToSyncedState()
	mem.SetIgnoreSyncs(false)
	r, err := pebblestore.Open(context.Background(), pebblestore.Config{Path: durPath}, pebblestore.WithFS(mem))
	require.NoError(t, err)
	return r
}

// TestDurable_AcknowledgedWritesAndObservedRowsSurviveCrash pins durable
// mode's contract under concurrency: after a crash that loses every unsynced
// byte, every write the store acknowledged and every row state a read
// returned is still there. Committing with pebble.NoSync instead of
// ApplyNoSyncWait+SyncWait fails it (acknowledged writes are lost).
func TestDurable_AcknowledgedWritesAndObservedRowsSurviveCrash(t *testing.T) {
	ctx := context.Background()
	mem := newStrictMem(t)
	s, err := pebblestore.Open(ctx, pebblestore.Config{Path: durPath}, pebblestore.WithFS(mem))
	require.NoError(t, err)

	_, rows := storetest.MintShard(t, s, "main", 256, store.ShardMined)
	exp := time.Now().Add(time.Hour)

	type obs struct {
		op  store.Outpoint
		tok store.Token
	}
	var (
		mu       sync.Mutex
		acked    []obs
		observed []obs
		wg       sync.WaitGroup
	)
	const writers = 16
	for w := 0; w < writers; w++ {
		wg.Add(2)
		go func() { // writer: reserve its own stripe of rows one at a time
			defer wg.Done()
			for i := w; i < len(rows); i += writers {
				tok := store.NewToken()
				applied, err := s.Reserve(ctx, "main", storetest.ReserveOps(rows[i:i+1], tok, exp))
				if !assert.NoError(t, err) {
					return
				}
				mu.Lock()
				for _, a := range applied {
					acked = append(acked, obs{a.Outpoint, tok})
				}
				mu.Unlock()
			}
		}()
		go func() { // reader: record every reservation it sees
			defer wg.Done()
			for i := 0; i < 20; i++ {
				got, err := s.GetRows(ctx, "main", storetest.Ops(rows))
				if !assert.NoError(t, err) {
					return
				}
				mu.Lock()
				for _, r := range got {
					if r.State == store.StateReserved {
						observed = append(observed, obs{store.Outpoint{TxID: r.TxID, Vout: r.Vout}, r.ReservedBy})
					}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Len(t, acked, len(rows))

	s = crashAndReopen(t, mem, s, nil)
	defer func() { _ = s.Close() }()
	after := storetest.RowsByOutpoint(t, s, "main", storetest.Ops(rows))
	for _, a := range acked {
		r, ok := after[a.op]
		require.True(t, ok, "acknowledged write's row %v lost in the crash", a.op)
		require.Equal(t, store.StateReserved, r.State, "acknowledged reservation of %v lost in the crash", a.op)
		require.Equal(t, a.tok, r.ReservedBy)
	}
	for _, o := range observed {
		r := after[o.op]
		require.Equal(t, store.StateReserved, r.State, "a read returned %v reserved, but that state did not survive the crash", o.op)
		require.Equal(t, o.tok, r.ReservedBy)
	}
}

// TestDurable_ReadWaitsForEarlierBatchSync pins that a read never returns a
// row state whose batch is applied but not yet durable: W's Reserve is held
// in its WAL sync, a read of the same row is issued, and the machine
// "crashes" before the sync completes. Whatever the read returned before the
// crash must survive it. Without the read waiting for earlier batches (see
// guard.unlock), the read returns W's reservation at once and the crash then
// takes it back.
func TestDurable_ReadWaitsForEarlierBatchSync(t *testing.T) {
	ctx := context.Background()
	mem := newStrictMem(t)
	gfs := newGateFS(mem)
	s, err := pebblestore.Open(ctx, pebblestore.Config{Path: durPath}, pebblestore.WithFS(gfs))
	require.NoError(t, err)
	_, rows := storetest.MintShard(t, s, "main", 1, store.ShardMined)

	gfs.hold()
	tok := store.NewToken()
	wDone := make(chan error, 1)
	go func() {
		_, err := s.Reserve(ctx, "main", storetest.ReserveOps(rows, tok, time.Now().Add(time.Hour)))
		wDone <- err
	}()
	select {
	case <-gfs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Reserve's WAL sync never started")
	}
	time.Sleep(20 * time.Millisecond) // let W's batch reach the memtable (it is published right after the WAL write)

	type readResult struct {
		rows []store.Row
		err  error
	}
	rDone := make(chan readResult, 1)
	go func() {
		got, err := s.GetRows(ctx, "main", storetest.Ops(rows))
		rDone <- readResult{got, err}
	}()
	var before *readResult
	select {
	case r := <-rDone:
		before = &r
	case <-time.After(200 * time.Millisecond): // still waiting for W's sync: the correct behaviour
	}
	select {
	case err := <-wDone:
		t.Fatalf("Reserve returned (%v) before its WAL sync completed", err)
	default:
	}

	s = crashAndReopen(t, mem, s, func() {
		gfs.open() // W's sync now runs, but is ignored: the crash already happened
		<-wDone
		if before == nil {
			<-rDone
		}
	})
	defer func() { _ = s.Close() }()

	after := storetest.RowsByOutpoint(t, s, "main", storetest.Ops(rows))
	assert.Equal(t, store.StateAvailable, after[storetest.Ops(rows)[0]].State, "W was never acknowledged before the crash and its sync was lost")
	if before != nil {
		require.NoError(t, before.err)
		require.Len(t, before.rows, 1)
		got := before.rows[0]
		assert.Equal(t, after[storetest.Ops(rows)[0]].State, got.State,
			"a read returned state %v before the crash, but after it the row is %v: the read observed a batch that was not yet durable", got.State, after[storetest.Ops(rows)[0]].State)
	}
}

// TestDurable_SyncFailureIsSurfaced pins that a failed WAL sync is reported
// as store.ErrUnavailable, both to the write whose batch it was and to a
// later read (which may have observed that batch's never-durable state).
func TestDurable_SyncFailureIsSurfaced(t *testing.T) {
	ctx := context.Background()
	gfs := newGateFS(newStrictMem(t))
	s, err := pebblestore.Open(ctx, pebblestore.Config{Path: durPath}, pebblestore.WithFS(gfs))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, rows := storetest.MintShard(t, s, "main", 1, store.ShardMined)

	errDisk := errors.New("injected WAL sync failure")
	gfs.fail(errDisk)
	_, err = s.Reserve(ctx, "main", storetest.ReserveOps(rows, store.NewToken(), time.Now().Add(time.Hour)))
	require.ErrorIs(t, err, store.ErrUnavailable)
	require.ErrorIs(t, err, errDisk)

	_, err = s.GetRows(ctx, "main", storetest.Ops(rows))
	require.ErrorIs(t, err, store.ErrUnavailable, "a read after a failed sync must not return state that never became durable")
	require.ErrorIs(t, err, errDisk)
}
