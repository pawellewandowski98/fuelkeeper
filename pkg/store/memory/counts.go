package memory

import "github.com/bsv-blockchain/fuelkeeper/pkg/store"

// countsFor returns pool's row-state counter array, creating and zero-
// initialising it on first use. Callers must hold s.mu.
func (s *Store) countsFor(pool string) *[5]int64 {
	c := s.counts[pool]
	if c == nil {
		c = &[5]int64{}
		s.counts[pool] = c
	}
	return c
}

// addCount adjusts pool's counter for state st by delta, for a row that
// enters the store directly in st rather than transitioning from another
// state (PutShard's insert). Callers must hold s.mu.
func (s *Store) addCount(pool string, st store.State, delta int64) {
	s.countsFor(pool)[st] += delta
}

// moveCount records one row's transition from state "from" to state "to" in
// pool, keeping Stats's per-state counters in sync without a full scan of
// s.rows. Callers must hold s.mu and must call this for every transition,
// including a terminal one (e.g. into StateSpent or StateInvalid): counters
// are never decremented back out of a terminal state, matching that spent
// and invalid rows are kept, not pruned. from == to (e.g. a same-token
// Reserve replay, which callers skip anyway) is harmless: the increment and
// decrement cancel out.
func (s *Store) moveCount(pool string, from, to store.State) {
	c := s.countsFor(pool)
	c[from]--
	c[to]++
}
