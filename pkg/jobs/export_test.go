package jobs

import "time"

// ReaperInputMismatch exposes the reaper's input-mismatch counter to tests.
var ReaperInputMismatch = reaperInputMismatch

// PoolRefilling exposes the minter's per-pool refill gauge to tests.
var PoolRefilling = poolRefilling

// ReaperRequeued exposes the reaper's requeue counter (by reason) to tests.
var ReaperRequeued = reaperRequeued

// ReaperSpent exposes the reaper's spent counter (by source) to tests.
var ReaperSpent = reaperSpent

// ReaperHeld exposes the reaper's held gauge (by reason) to tests.
var ReaperHeld = reaperHeld

// ReaperResolved exposes the reaper's resolved counter (by target state) to tests.
var ReaperResolved = reaperResolved

// SetHintCheckCap overrides the reaper's hint recheck map capacity.
func SetHintCheckCap(r *Reaper, n int) { r.hints.cap = n }

// HintChecks returns how many txids the reaper's hint recheck map holds.
func HintChecks(r *Reaper) int { return len(r.hints.m) }

// PruneHintChecks drops recheck entries due at or before now.
func PruneHintChecks(r *Reaper, now time.Time) { r.hints.prune(now) }
