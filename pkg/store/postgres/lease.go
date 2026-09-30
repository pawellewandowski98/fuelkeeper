package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// TryAcquireOrRenew implements store.Leaser.
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if err := store.ValidateLease(name, holder, ttl); err != nil {
		return time.Time{}, err
	}
	now := s.now()
	var exp time.Time
	err := s.pool.QueryRow(ctx, `
		INSERT INTO fuel_leases (name, holder, expires_at) VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE
		   SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
		 WHERE fuel_leases.holder = EXCLUDED.holder OR fuel_leases.expires_at <= $4
		RETURNING expires_at`, name, holder, now.Add(ttl), now).Scan(&exp)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil // lost: another holder owns an unexpired lease
	}
	if err != nil {
		return time.Time{}, wrap(err)
	}
	return exp.UTC(), nil
}

// ReleaseLease implements store.Leaser.
func (s *Store) ReleaseLease(ctx context.Context, name, holder string) error {
	if err := store.ValidateLeaseName(name); err != nil {
		return err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM fuel_leases WHERE name = $1 AND holder = $2`, name, holder)
	return wrap(err)
}
