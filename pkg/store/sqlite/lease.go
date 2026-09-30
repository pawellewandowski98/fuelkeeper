package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// TryAcquireOrRenew implements store.Leaser. The lease-free predicate is
// expires_at <= now (a job lease is free at exactly its expiry instant).
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if err := store.ValidateLease(name, holder, ttl); err != nil {
		return time.Time{}, err
	}
	now := s.now()
	exp := now.Add(ttl)
	var got time.Time
	err := s.write(ctx, func(ctx context.Context, c querier) error {
		var curHolder string
		var curExp int64
		err := c.QueryRowContext(ctx, `SELECT holder, expires_at FROM fuel_leases WHERE name = ?`, name).Scan(&curHolder, &curExp)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			_, err = c.ExecContext(ctx, `INSERT INTO fuel_leases (name, holder, expires_at) VALUES (?, ?, ?)`, name, holder, exp.UnixMicro())
		case err != nil:
			return err
		case curHolder == holder || curExp <= now.UnixMicro():
			_, err = c.ExecContext(ctx, `UPDATE fuel_leases SET holder = ?, expires_at = ? WHERE name = ?`, holder, exp.UnixMicro(), name)
		default:
			return nil // lost: another holder owns an unexpired lease
		}
		if err != nil {
			return err
		}
		// Reconstruct from the stored microsecond value, not exp itself: the
		// contract requires every stored time to be truncated to microsecond
		// precision on write (store.go, "Clocks"), and exp may carry a
		// sub-microsecond remainder from ttl.
		got = time.UnixMicro(exp.UnixMicro()).UTC()
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return got, nil
}

// ReleaseLease implements store.Leaser.
func (s *Store) ReleaseLease(ctx context.Context, name, holder string) error {
	if err := store.ValidateLeaseName(name); err != nil {
		return err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return err
	}
	return s.write(ctx, func(ctx context.Context, c querier) error {
		_, err := c.ExecContext(ctx, `DELETE FROM fuel_leases WHERE name = ? AND holder = ?`, name, holder)
		return err
	})
}
