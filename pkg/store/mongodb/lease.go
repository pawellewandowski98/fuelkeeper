package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// TryAcquireOrRenew implements store.Leaser. The upsert's own filter only
// matches a document that is either already held by holder or expired; when
// neither is true, the FindOneAndUpdate falls through to an insert attempt,
// which then collides on _id with the existing (unexpired, other-holder)
// document. That duplicate-key error is exactly the signal that another
// holder owns an unexpired lease.
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
	octx, cancel := s.op(ctx)
	defer cancel()

	now := s.now().Truncate(time.Microsecond)
	exp := now.Add(ttl).Truncate(time.Microsecond)
	filter := bson.D{
		{Key: "_id", Value: name},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "h", Value: holder}},
			bson.D{{Key: "e", Value: bson.D{{Key: "$lte", Value: us(now)}}}},
		}},
	}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "h", Value: holder}, {Key: "e", Value: us(exp)}}}}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	var d leaseDoc
	err := s.leases.FindOneAndUpdate(octx, filter, update, opts).Decode(&d)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return time.Time{}, nil // lost: another holder owns an unexpired lease
		}
		return time.Time{}, wrap(err)
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
	if s.closed.Load() {
		return errClosed
	}
	octx, cancel := s.op(ctx)
	defer cancel()

	_, err := s.leases.DeleteOne(octx, bson.D{{Key: "_id", Value: name}, {Key: "h", Value: holder}})
	if err != nil {
		return wrap(err)
	}
	return nil
}
