package sqlite

// The SQL constants below re-export the exact statement text the Store
// methods send, so explain_test.go can EXPLAIN QUERY PLAN the statements this
// package actually ships instead of a hand-copied (and driftable)
// approximation.
const (
	ListAvailableSQL        = listAvailableSQL
	ReserveRowSQL           = reserveRowSQL
	ReleaseRowSQL           = releaseRowSQL
	SpendTerminalRowSQL     = spendTerminalRowSQL
	SpendHintRowSQL         = spendHintRowSQL
	QuarantineCandidateSQL  = quarantineCandidateSQL
	ListQuarantinedSQL      = listQuarantinedSQL
	ListQuarantinedAfterSQL = listQuarantinedAfterSQL
	ResolveAvailableRowSQL  = resolveAvailableRowSQL
	ResolveSpentRowSQL      = resolveSpentRowSQL
	RetireShardRowSQL       = retireShardRowSQL
)
