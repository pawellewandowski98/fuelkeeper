package pool_test

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// live tracks, per outpoint, the reservation the "network of clients" believes
// is live: token + expiry. A reservation stops being live when the client
// releases it, spends it, or its expiry passes on the manual clock. It also
// double-checks the store side of every claim: st/poolName let claim ask the
// store directly, not just the in-memory bookkeeping, whether an outpoint it
// was just handed is really reserved under that token.
type live struct {
	mu       sync.Mutex
	held     map[store.Outpoint]liveRes
	relIn    map[store.Outpoint]store.Token // Release call in progress for op under this token
	st       store.Store
	poolName string
}

type liveRes struct {
	tok store.Token
	exp time.Time
}

// storeGetRowsChunk is the largest batch claim passes to GetRows per call, at
// or below every backend's Caps.MaxBatch (memory's is 1000).
const storeGetRowsChunk = 1000

func (l *live) claim(ctx context.Context, t *testing.T, now time.Time, tok store.Token, exp time.Time, ops []store.Outpoint) {
	l.mu.Lock()
	for _, op := range ops {
		// A Release under prev.tok that is still in flight may already have
		// freed the row in the store (drop only runs once Release returns),
		// so a re-handout then is legitimate; the store-side check below
		// still verifies the new reservation.
		if prev, ok := l.held[op]; ok && prev.tok != tok && now.Before(prev.exp) && l.relIn[op] != prev.tok {
			t.Errorf("G1 violated: %s handed to %s while live under %s until %s (now %s)", op, tok, prev.tok, prev.exp, now)
		}
		l.held[op] = liveRes{tok: tok, exp: exp}
	}
	l.mu.Unlock()

	// Verify the store side too: Reserve must not report an outpoint it did
	// not durably record. Skip once the reservation is already past its own
	// expiry by our own clock reading: a delayed goroutine may find the
	// reaper already quarantined it, which is not a violation of anything.
	if !now.Before(exp) {
		return
	}
	for i := 0; i < len(ops); i += storeGetRowsChunk {
		end := min(i+storeGetRowsChunk, len(ops))
		batch := ops[i:end]
		rows, err := l.st.GetRows(ctx, l.poolName, batch)
		if err != nil {
			t.Errorf("G1 store check: GetRows: %v", err)
			continue
		}
		byOp := make(map[store.Outpoint]store.Row, len(rows))
		for _, r := range rows {
			byOp[r.Outpoint] = r
		}
		for _, op := range batch {
			r, ok := byOp[op]
			switch {
			case !ok:
				t.Errorf("G1 store check: %s missing from GetRows right after Reserve returned it under %s", op, tok)
			case r.State != store.StateReserved || r.ReservedBy != tok:
				t.Errorf("G1 store check: %s state=%s reservedBy=%s, want reserved by %s", op, r.State, r.ReservedBy, tok)
			}
		}
	}
}

// mark records that a Release of ops under tok is about to start.
func (l *live) mark(tok store.Token, ops []store.Outpoint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.relIn == nil {
		l.relIn = map[store.Outpoint]store.Token{}
	}
	for _, op := range ops {
		l.relIn[op] = tok
	}
}

// unmark clears mark's record, only where it is still tok's.
func (l *live) unmark(tok store.Token, ops []store.Outpoint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, op := range ops {
		if l.relIn[op] == tok {
			delete(l.relIn, op)
		}
	}
}

func (l *live) drop(ops []store.Outpoint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, op := range ops {
		delete(l.held, op)
	}
}

func (l *live) expire(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for op, r := range l.held {
		if !now.Before(r.exp) {
			delete(l.held, op)
		}
	}
}

// crashableStore wraps the shared store for one instance: once crashed, lease
// releases become no-ops so the instance's shard leases linger until they
// expire, exactly as after a process crash.
type crashableStore struct {
	store.Store
	crashed atomic.Bool
}

func (c *crashableStore) ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error {
	if c.crashed.Load() {
		return nil
	}
	return c.Store.ReleaseShard(ctx, pool, txid, holder)
}

func (c *crashableStore) ReleaseLease(ctx context.Context, name, holder string) error {
	if c.crashed.Load() {
		return nil
	}
	return c.Store.ReleaseLease(ctx, name, holder)
}

type instance struct {
	p      *pool.Pool
	st     *crashableStore
	cancel context.CancelFunc
	done   chan struct{}
}

func startInstance(t *testing.T, shared store.Store, pc pool.Config, clk *storetest.Clock, id string) *instance {
	t.Helper()
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = id
	ic.MaxShards = 3
	ic.LowWaterRows = 20
	ic.LeaseTTL = 30 * time.Second
	ic.RescanInterval = 5 * time.Second
	ic.AdoptTimeout = 50 * time.Millisecond
	cs := &crashableStore{Store: shared}
	p := pool.NewPool(cs, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	return &instance{p: p, st: cs, cancel: cancel, done: done}
}

func (in *instance) stop() { in.cancel(); <-in.done }

func (in *instance) crash() { in.st.crashed.Store(true); in.stop() }

func stressMul() int {
	if v, err := strconv.Atoi(os.Getenv("FUELKEEPER_STRESS")); err == nil && v > 0 {
		return v
	}
	return 1
}

func TestInvariantsMultiInstance(t *testing.T) {
	ctx := context.Background()
	rounds := 300 * stressMul()
	seed := time.Now().UnixNano()
	if v, err := strconv.ParseInt(os.Getenv("FUELKEEPER_SEED"), 10, 64); err == nil {
		seed = v // e.g. FUELKEEPER_SEED=42 to replay a specific run
	}
	t.Logf("seed=%d rounds=%d", seed, rounds)
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic simulation

	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = 5 * time.Second
	pc.DefaultTTL = 20 * time.Second
	pc.MaxTTL = time.Minute
	require.NoError(t, pc.Validate())

	// Seed 6×stressMul() mined shards × 40 rows; the oracle knows them all as
	// unspent. Supply scales with stress so a higher FUELKEEPER_STRESS still
	// has fuel to hand out after a few hundred rounds (see the re-mint below).
	total := 0
	for i := 0; i < 6*stressMul(); i++ {
		_, rows := storetest.MintShard(t, st, "main", 40, store.ShardMined)
		total += len(rows)
		for _, r := range rows {
			orc.SetUnspent(r.Outpoint)
		}
	}

	rcfg := jobs.DefaultReaperConfig()
	rcfg.Grace = 2 * time.Second
	rcfg.QuarantineMinAge = 2 * time.Second
	rcfg.MinUnspentAge = 2 * time.Second
	rcfg.HintPatience = 10 * time.Second
	reaper, err := jobs.NewReaper(st, orc, []string{"main"}, rcfg, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	instances := []*instance{startInstance(t, st, pc, clk, "i0"), startInstance(t, st, pc, clk, "i1"), startInstance(t, st, pc, clk, "i2")}
	t.Cleanup(func() {
		for _, in := range instances {
			in.stop()
		}
	})
	for _, in := range instances {
		in.p.EnsureRows(ctx) // let managers adopt before the storm
	}

	lv := &live{held: map[store.Outpoint]liveRes{}, st: st, poolName: pc.Name}
	type held struct {
		tok store.Token
		ops []store.Outpoint
	}
	var mine []held
	var nReserve, nRelease, nSpend, nAbandon, nTick, nCrash, nMint int

	for round := 0; round < rounds; round++ {
		if round > 0 && round%100 == 0 {
			// Keep the pool fed: without this, a high FUELKEEPER_STRESS
			// multiplier can burn through the fixed initial supply well
			// before the run ends.
			_, rows := storetest.MintShard(t, st, "main", 40, store.ShardMined)
			total += len(rows)
			for _, r := range rows {
				orc.SetUnspent(r.Outpoint)
			}
			nMint++
		}
		action := rng.Intn(100)
		in := instances[rng.Intn(len(instances))]
		switch {
		case action < 55: // reserve
			res, err := in.p.Reserve(ctx, pool.ReserveRequest{Count: 1 + rng.Intn(5), MinCount: 1})
			if err != nil {
				continue // exhausted / insufficient / backpressure are legitimate under load
			}
			nReserve++
			ops := outpointsOf(res.Outputs)
			lv.claim(ctx, t, clk.Now(), res.Token, res.ExpiresAt, ops)
			mine = append(mine, held{tok: res.Token, ops: ops})
		case action < 70 && len(mine) > 0: // release
			i := rng.Intn(len(mine))
			h := mine[i]
			n, err := in.p.Release(ctx, h.tok, h.ops)
			require.NoError(t, err)
			if n > 0 {
				lv.drop(h.ops)
			}
			mine = append(mine[:i], mine[i+1:]...)
			nRelease++
		case action < 85 && len(mine) > 0: // spend on-chain then notify
			i := rng.Intn(len(mine))
			h := mine[i]
			spender := storetest.NewTxID()
			for _, op := range h.ops {
				orc.SetSpent(op, spender)
			}
			orc.SetTx(spender, oracle.TxStatus{Known: true, Mined: true})
			_, err := in.p.Spend(ctx, h.tok, h.ops, spender)
			require.NoError(t, err)
			lv.drop(h.ops)
			mine = append(mine[:i], mine[i+1:]...)
			nSpend++
		case action < 92 && len(mine) > 0: // abandon: expiry will handle it (row stays unspent on-chain)
			i := rng.Intn(len(mine))
			mine = append(mine[:i], mine[i+1:]...)
			nAbandon++
		case action < 96: // time passes; reaper runs
			clk.Advance(3 * time.Second)
			lv.expire(clk.Now())
			require.NoError(t, reaper.RunOnce(ctx))
			nTick++
		default: // crash an instance (leases linger) and start a replacement
			idx := rng.Intn(len(instances))
			instances[idx].crash()
			instances[idx] = startInstance(t, st, pc, clk, "r"+strconv.Itoa(round))
			nCrash++
		}
	}
	t.Logf("reserve=%d release=%d spend=%d abandon=%d tick=%d crash=%d mint=%d", nReserve, nRelease, nSpend, nAbandon, nTick, nCrash, nMint)

	// Drain: release everything still held, advance past every TTL and lease, reap until stable.
	for _, h := range mine {
		_, _ = instances[0].p.Release(ctx, h.tok, h.ops)
	}
	for i := 0; i < 10; i++ {
		clk.Advance(30 * time.Second)
		require.NoError(t, reaper.RunOnce(ctx))
	}

	// G2: every row is in exactly one state, nothing is lost, and after draining
	// every row is available or provably spent.
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	sum := stats.Available + stats.Reserved + stats.Quarantined + stats.Spent + stats.Invalid
	require.Equal(t, int64(total), sum, "rows conserved")
	require.Equal(t, int64(0), stats.Reserved, "no reservation survives the drain")
	require.Equal(t, int64(0), stats.Quarantined, "every quarantined row resolved (oracle knew every outpoint)")
	require.Equal(t, int64(0), stats.Invalid)
	require.Positive(t, stats.Spent, "the simulation spent something")
	require.Positive(t, stats.Available, "the simulation left fuel available (supply scales with stress)")
	t.Logf("final: available=%d spent=%d", stats.Available, stats.Spent)
}

// TestInvariantsConcurrentClients runs the same two-instance-store setup as
// TestInvariantsMultiInstance, but with genuinely concurrent clients: no
// crashes, no clock advances inside the client goroutines. A single ticker
// goroutine owns the manual clock and the reaper, so RunOnce is never called
// concurrently with itself; the client goroutines only ever call Reserve,
// Release and Spend, which the Pool must already serialize safely.
func TestInvariantsConcurrentClients(t *testing.T) {
	ctx := context.Background()

	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = 5 * time.Second
	pc.DefaultTTL = 20 * time.Second
	pc.MaxTTL = time.Minute
	require.NoError(t, pc.Validate())

	total := 0
	for i := 0; i < 6*stressMul(); i++ {
		_, rows := storetest.MintShard(t, st, "main", 40, store.ShardMined)
		total += len(rows)
		for _, r := range rows {
			orc.SetUnspent(r.Outpoint)
		}
	}

	rcfg := jobs.DefaultReaperConfig()
	rcfg.Grace = 2 * time.Second
	rcfg.QuarantineMinAge = 2 * time.Second
	rcfg.MinUnspentAge = 2 * time.Second
	rcfg.HintPatience = 10 * time.Second
	reaper, err := jobs.NewReaper(st, orc, []string{"main"}, rcfg, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	instances := []*instance{startInstance(t, st, pc, clk, "c0"), startInstance(t, st, pc, clk, "c1"), startInstance(t, st, pc, clk, "c2")}
	t.Cleanup(func() {
		for _, in := range instances {
			in.stop()
		}
	})
	for _, in := range instances {
		in.p.EnsureRows(ctx) // let managers adopt before the storm
	}

	lv := &live{held: map[store.Outpoint]liveRes{}, st: st, poolName: pc.Name}

	// Ticker goroutine: the sole owner of the manual clock and the reaper.
	// Being a single goroutine looping over one ticker channel guarantees
	// RunOnce is never invoked concurrently with itself.
	stopTicker := make(chan struct{})
	tickerDone := make(chan struct{})
	go func() {
		defer close(tickerDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTicker:
				return
			case <-ticker.C:
				clk.Advance(3 * time.Second)
				lv.expire(clk.Now())
				assert.NoError(t, reaper.RunOnce(ctx))
			}
		}
	}()

	type held struct {
		tok store.Token
		ops []store.Outpoint
	}

	const clients = 8
	const roundsPerClient = 100
	var wg sync.WaitGroup
	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic per-goroutine simulation
			var mine []held
			for round := 0; round < roundsPerClient; round++ {
				in := instances[rng.Intn(len(instances))]
				action := rng.Intn(100)
				switch {
				case action < 55: // reserve
					res, err := in.p.Reserve(ctx, pool.ReserveRequest{Count: 1 + rng.Intn(5), MinCount: 1})
					if err != nil {
						continue // exhausted / insufficient / backpressure are legitimate under load
					}
					ops := outpointsOf(res.Outputs)
					lv.claim(ctx, t, clk.Now(), res.Token, res.ExpiresAt, ops)
					mine = append(mine, held{tok: res.Token, ops: ops})
				case action < 75 && len(mine) > 0: // release
					i := rng.Intn(len(mine))
					h := mine[i]
					lv.mark(h.tok, h.ops)
					n, err := in.p.Release(ctx, h.tok, h.ops)
					assert.NoError(t, err)
					if n > 0 {
						lv.drop(h.ops)
					}
					lv.unmark(h.tok, h.ops)
					mine = append(mine[:i], mine[i+1:]...)
				case len(mine) > 0: // spend on-chain then notify
					i := rng.Intn(len(mine))
					h := mine[i]
					spender := storetest.NewTxID()
					for _, op := range h.ops {
						orc.SetSpent(op, spender)
					}
					orc.SetTx(spender, oracle.TxStatus{Known: true, Mined: true})
					_, err := in.p.Spend(ctx, h.tok, h.ops, spender)
					assert.NoError(t, err)
					lv.drop(h.ops)
					mine = append(mine[:i], mine[i+1:]...)
				}
			}
			// Drain this goroutine's own remaining reservations before returning.
			drainInstance := instances[0]
			for _, h := range mine {
				lv.mark(h.tok, h.ops)
				n, err := drainInstance.p.Release(ctx, h.tok, h.ops)
				assert.NoError(t, err)
				if n > 0 {
					lv.drop(h.ops)
				}
				lv.unmark(h.tok, h.ops)
			}
		}(int64(1000 + c))
	}
	wg.Wait()
	close(stopTicker)
	<-tickerDone

	// Drain: advance past every TTL and lease, reap until stable.
	for i := 0; i < 10; i++ {
		clk.Advance(30 * time.Second)
		require.NoError(t, reaper.RunOnce(ctx))
	}

	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	sum := stats.Available + stats.Reserved + stats.Quarantined + stats.Spent + stats.Invalid
	require.Equal(t, int64(total), sum, "rows conserved")
	require.Equal(t, int64(0), stats.Reserved, "no reservation survives the drain")
	require.Equal(t, int64(0), stats.Quarantined, "every quarantined row resolved (oracle knew every outpoint)")
	require.Equal(t, int64(0), stats.Invalid)
	require.Positive(t, stats.Spent, "the simulation spent something")
	require.Positive(t, stats.Available, "the simulation left fuel available (supply scales with stress)")
	t.Logf("final: available=%d spent=%d", stats.Available, stats.Spent)
}
