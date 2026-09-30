package pebble

import (
	"context"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// loadLease reads and decodes a lease, returning (nil, nil) when it does not exist.
func (s *Store) loadLease(name string) (*leaseDoc, error) {
	v, err := s.get(leaseKey(name))
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil //nolint:nilnil // "not found" is a valid, non-error result here
	}
	d, err := decodeLease(v)
	if err != nil {
		return nil, wrapErr(err)
	}
	return &d, nil
}

// TryAcquireOrRenew implements store.Leaser.
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if err := store.ValidateLease(name, holder, ttl); err != nil {
		return time.Time{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return time.Time{}, errClosed
	}
	now := s.now()
	cur, err := s.loadLease(name)
	if err != nil {
		return time.Time{}, err
	}
	if cur != nil && cur.Holder != holder && fromUS(cur.Expires).After(now) {
		return time.Time{}, nil
	}
	exp := now.Add(ttl).Truncate(time.Microsecond)
	next := leaseDoc{Holder: holder, Expires: us(exp)}
	if err := s.db.Set(leaseKey(name), encodeLease(&next), s.wo); err != nil {
		return time.Time{}, wrapErr(err)
	}
	return exp, nil
}

// ReleaseLease implements store.Leaser.
func (s *Store) ReleaseLease(ctx context.Context, name, holder string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.ValidateLeaseName(name); err != nil {
		return err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errClosed
	}
	cur, err := s.loadLease(name)
	if err != nil {
		return err
	}
	if cur == nil || cur.Holder != holder {
		return nil
	}
	if err := s.db.Delete(leaseKey(name), s.wo); err != nil {
		return wrapErr(err)
	}
	return nil
}
