package aerospike

import (
	"context"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// TryAcquireOrRenew implements store.Leaser. A CREATE_ONLY insert wins the
// lease outright when no record exists yet; on KEY_EXISTS_ERROR, a follow-up
// UPDATE_ONLY write guarded by "holder == me || exp <= now" either renews
// (this holder already owns it, or it expired) or reports FILTERED_OUT (a
// different holder owns an unexpired lease), which this returns as the zero
// time rather than an error (store.go: "Errors are for infrastructure
// failures only").
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if err := store.ValidateLease(name, holder, ttl); err != nil {
		return time.Time{}, err
	}
	if s.closed.Load() {
		return time.Time{}, errClosed
	}

	key, err := s.leaseKey(name)
	if err != nil {
		return time.Time{}, err
	}
	now := s.now().Truncate(time.Microsecond)
	exp := now.Add(ttl).Truncate(time.Microsecond)
	bins := as.BinMap{binHolder: holder, binExp: us(exp)}

	createErr := s.client.Put(s.createPolicy(ctx), key, bins)
	if createErr == nil {
		return exp, nil
	}
	if !createErr.Matches(types.KEY_EXISTS_ERROR) {
		return time.Time{}, wrap(createErr)
	}

	wp := s.writePolicy(ctx)
	wp.FilterExpression = as.ExpOr(
		as.ExpEq(as.ExpStringBin(binHolder), as.ExpStringVal(holder)),
		as.ExpLessEq(as.ExpIntBin(binExp), as.ExpIntVal(us(now))),
	)
	aerr := s.client.Put(wp, key, bins)
	switch {
	case aerr == nil:
		return exp, nil
	case isFiltered(aerr):
		return time.Time{}, nil // another holder owns an unexpired lease
	default:
		return time.Time{}, wrap(aerr)
	}
}

// ReleaseLease implements store.Leaser: a guarded delete that is a no-op
// unless holder currently owns the lease.
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
	if s.closed.Load() {
		return errClosed
	}

	key, err := s.leaseKey(name)
	if err != nil {
		return err
	}
	wp := s.writePolicy(ctx)
	wp.FilterExpression = as.ExpEq(as.ExpStringBin(binHolder), as.ExpStringVal(holder))
	_, aerr := s.client.Delete(wp, key)
	if aerr == nil || isFiltered(aerr) || isKeyNotFound(aerr) {
		return nil
	}
	return wrap(aerr)
}
