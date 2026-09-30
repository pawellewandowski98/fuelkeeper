package jobs

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
