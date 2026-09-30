package store

import (
	"bytes"
	"sort"
)

// LessOutpoint orders outpoints by their txid's 32 internal bytes, then vout.
// Every backend's batch mutations (Reserve, Release, Spend, Resolve,
// RetireShard's row cascade) process their ops in this order, one row at a
// time, inside a single transaction: two concurrent batches touching
// overlapping rows then always request their per-row locks in the same
// global order, which rules out a deadlock a combined multi-row statement can
// hit when the database's own planner doesn't happen to honour the caller's
// input order.
func LessOutpoint(a, b Outpoint) bool {
	if c := bytes.Compare(a.TxID[:], b.TxID[:]); c != 0 {
		return c < 0
	}
	return a.Vout < b.Vout
}

// SortOutpoints returns a copy of ops sorted by LessOutpoint.
func SortOutpoints(ops []Outpoint) []Outpoint {
	out := make([]Outpoint, len(ops))
	copy(out, ops)
	sort.Slice(out, func(i, j int) bool { return LessOutpoint(out[i], out[j]) })
	return out
}

// SortReserveOps returns a copy of ops sorted by LessOutpoint on their Outpoint.
func SortReserveOps(ops []ReserveOp) []ReserveOp {
	out := make([]ReserveOp, len(ops))
	copy(out, ops)
	sort.Slice(out, func(i, j int) bool { return LessOutpoint(out[i].Outpoint, out[j].Outpoint) })
	return out
}
