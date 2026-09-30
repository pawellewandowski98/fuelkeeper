package mongodb

// RetireCascadeFilter exposes RetireShard's own cascade filter for tests
// (indexes_test.go's index/explain assertions), so they explain the exact
// query the code issues rather than a hand-maintained copy that can drift
// from it. It lives here, in an untagged _test.go file, rather than as an
// exported function in shards.go: a _test.go file is never part of the
// production build regardless of which build tags are active, so this
// test-only surface is not exported from the compiled package.
var RetireCascadeFilter = retireCascadeFilter
