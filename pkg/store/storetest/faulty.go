package storetest

import (
	"context"
	"errors"
	"sync"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// ErrInjected is the error Faulty returns when a fault fires.
var ErrInjected = errors.New("storetest: injected failure")

// Faulty wraps a Store and injects failures into Reserve. Every other method is
// delegated unchanged.
//
//   - FailAfterApplying(n): the next Reserve applies only the first n ops, then
//     returns ErrInjected wrapped in store.ErrUnavailable (partial apply + lost
//     response).
//   - InDoubtNext(n): the next n Reserve calls apply fully but report
//     store.ErrUnavailable (commit succeeded, response lost).
//   - InDoubtEvery(k): every k-th Reserve (by call count) behaves the same way.
//   - BeforeReserve / AfterReserve hooks run around every Reserve (for pausing).
//
// BeforeReserve and AfterReserve are plain fields, not synchronized: set them
// once, before the wrapper is handed to anything that might call Reserve
// concurrently (e.g. before starting a Coalescer's Run). Assigning them
// concurrently with an in-flight Reserve call is a data race.
type Faulty struct {
	store.Store
	mu            sync.Mutex
	failAfter     int // -1 = off
	inDoubtNext   int // remaining one-shot in-doubt calls
	inDoubtEvery  int // 0 = off
	reserveCalls  int
	BeforeReserve func(ops []store.ReserveOp)
	AfterReserve  func(applied []store.ReserveOp, err error)
}

// NewFaulty wraps s.
func NewFaulty(s store.Store) *Faulty { return &Faulty{Store: s, failAfter: -1} }

// FailAfterApplying arms a one-shot partial failure.
func (f *Faulty) FailAfterApplying(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAfter = n
}

// InDoubtNext makes the next n Reserve calls report ErrUnavailable after committing.
func (f *Faulty) InDoubtNext(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inDoubtNext = n
}

// InDoubtEvery makes every k-th Reserve call report ErrUnavailable after committing (0 = off).
func (f *Faulty) InDoubtEvery(k int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inDoubtEvery = k
}

// ReserveCalls returns how many Reserve calls reached the wrapper.
func (f *Faulty) ReserveCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reserveCalls
}

// Reserve applies the armed fault, if any, around the wrapped Reserve.
func (f *Faulty) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	if f.BeforeReserve != nil {
		f.BeforeReserve(ops)
	}
	f.mu.Lock()
	f.reserveCalls++
	calls := f.reserveCalls
	failAfter := f.failAfter
	f.failAfter = -1
	inDoubt := f.inDoubtEvery > 0 && calls%f.inDoubtEvery == 0
	if f.inDoubtNext > 0 {
		f.inDoubtNext--
		inDoubt = true
	}
	f.mu.Unlock()

	var applied []store.ReserveOp
	var err error
	switch {
	case failAfter >= 0:
		if failAfter > 0 {
			_, err = f.Store.Reserve(ctx, pool, ops[:min(failAfter, len(ops))])
		}
		if err == nil {
			err = errors.Join(store.ErrUnavailable, ErrInjected)
		}
		// applied stays nil: the caller never learns what applied
	default:
		applied, err = f.Store.Reserve(ctx, pool, ops)
		if err == nil && inDoubt {
			applied, err = nil, errors.Join(store.ErrUnavailable, ErrInjected)
		}
	}
	if f.AfterReserve != nil {
		f.AfterReserve(applied, err)
	}
	return applied, err
}

var _ store.Store = (*Faulty)(nil)
