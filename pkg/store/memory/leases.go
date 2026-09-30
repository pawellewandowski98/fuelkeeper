package memory

import (
	"context"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// TryAcquireOrRenew implements store.Leaser.
func (s *Store) TryAcquireOrRenew(_ context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if err := store.ValidateLease(name, holder, ttl); err != nil {
		return time.Time{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	l, ok := s.leases[name]
	if ok && l.holder != holder && l.expires.After(now) {
		return time.Time{}, nil
	}
	// Truncate the stored/returned expiry to microsecond precision (store.go,
	// "Clocks"): ttl itself may carry a sub-microsecond remainder even when now
	// doesn't.
	exp := now.Add(ttl).Truncate(time.Microsecond)
	s.leases[name] = lease{holder: holder, expires: exp}
	return exp, nil
}

// ReleaseLease implements store.Leaser.
func (s *Store) ReleaseLease(_ context.Context, name, holder string) error {
	if err := store.ValidateLeaseName(name); err != nil {
		return err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.leases[name]; ok && l.holder == holder {
		delete(s.leases, name)
	}
	return nil
}
