//go:build postgres

package postgres

import "github.com/jackc/pgx/v5/pgxpool"

// Pool exposes the underlying pool for tests and diagnostics. It lives here,
// not in postgres.go, because nothing outside this package's own tests needs
// it: production code always goes through the Store methods.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// The SQL constants below re-export the exact statement text the Store
// methods send, so explain_test.go can EXPLAIN the statements this package
// actually ships instead of a hand-copied (and driftable) approximation.
const (
	ListAvailableSQL        = listAvailableSQL
	GetRowsSQL              = getRowsSQL
	ReserveRowSQL           = reserveRowSQL
	ReleaseRowSQL           = releaseRowSQL
	SpendTerminalRowSQL     = spendTerminalRowSQL
	SpendHintRowSQL         = spendHintRowSQL
	QuarantineSQL           = quarantineSQL
	ListQuarantinedSQL      = listQuarantinedSQL
	ListQuarantinedAfterSQL = listQuarantinedAfterSQL
	ResolveAvailableRowSQL  = resolveAvailableRowSQL
	ResolveSpentRowSQL      = resolveSpentRowSQL
	AcquireShardSQL         = acquireShardSQL
	RetireShardRowSQL       = retireShardRowSQL
)
