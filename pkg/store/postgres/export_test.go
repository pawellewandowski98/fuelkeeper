//go:build postgres

package postgres

import "github.com/jackc/pgx/v5/pgxpool"

// Pool exposes the underlying pool for tests and diagnostics. It lives here,
// not in postgres.go, because nothing outside this package's own tests needs
// it: production code always goes through the Store methods.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
