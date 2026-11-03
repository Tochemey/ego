// MIT License
//
// Copyright (c) 2022-2026 Arsene Tochemey Gandote
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cassandra

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"golang.org/x/sync/errgroup"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/offsetstore"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// offsetsPerBatch is how many offsets one logged batch writes or deletes.
	// Every offset is two statements, one per table, so a batch stays well
	// below Cassandra's batch size limits.
	offsetsPerBatch = 50
	// shardQueryConcurrency bounds how many per-shard queries run at once.
	shardQueryConcurrency = 32
	// maxPreallocatedRows caps the capacity a read reserves up front from its
	// limit, so a large limit does not allocate memory the result never fills.
	maxPreallocatedRows = 1024

	// insertOffsetStatement stores an offset in offsets_store. A Cassandra
	// INSERT is an upsert, so an offset written again keeps a single row.
	insertOffsetStatement = `INSERT INTO offsets_store (projection_name, shard_number, timestamp, persistence_id, sequence_number, consumed_at) VALUES (?, ?, ?, ?, ?, ?)`
	// insertEntityOffsetStatement stores the same offset in offsets_by_entity.
	insertEntityOffsetStatement = `INSERT INTO offsets_by_entity (projection_name, shard_number, persistence_id, sequence_number, timestamp, consumed_at) VALUES (?, ?, ?, ?, ?, ?)`
	// readOffsetsStatement reads the first rows of a projection's shard
	// partition, which hold the newest timestamps.
	readOffsetsStatement = `SELECT persistence_id, sequence_number, timestamp, consumed_at FROM offsets_store WHERE projection_name = ? AND shard_number = ? LIMIT ?`
	// readEntityOffsetStatement reads the offset of a persistence ID with the
	// highest sequence number.
	readEntityOffsetStatement = `SELECT sequence_number, timestamp, consumed_at FROM offsets_by_entity WHERE projection_name = ? AND shard_number = ? AND persistence_id = ? LIMIT 1`
	// latestOffsetStatement reads the newest timestamp of a projection's shard partition.
	latestOffsetStatement = `SELECT timestamp FROM offsets_store WHERE projection_name = ? AND shard_number = ? LIMIT 1`
	// offsetsBeforeStatement reads the offsets of a projection's shard
	// partition stamped before a timestamp.
	offsetsBeforeStatement = `SELECT timestamp, persistence_id, sequence_number, consumed_at FROM offsets_store WHERE projection_name = ? AND shard_number = ? AND timestamp < ?`
	// deleteOffsetStatement removes an offset from offsets_store.
	deleteOffsetStatement = `DELETE FROM offsets_store WHERE projection_name = ? AND shard_number = ? AND timestamp = ? AND persistence_id = ? AND sequence_number = ?`
	// deleteEntityOffsetStatement removes an offset from offsets_by_entity.
	deleteEntityOffsetStatement = `DELETE FROM offsets_by_entity WHERE projection_name = ? AND shard_number = ? AND persistence_id = ? AND sequence_number = ?`
	// deleteShardOffsetsStatement removes a projection's shard partition from offsets_store.
	deleteShardOffsetsStatement = `DELETE FROM offsets_store WHERE projection_name = ? AND shard_number = ?`
	// deleteShardEntityOffsetsStatement removes a projection's shard partition from offsets_by_entity.
	deleteShardEntityOffsetsStatement = `DELETE FROM offsets_by_entity WHERE projection_name = ? AND shard_number = ?`
	// writeStartOffsetStatement records the timestamp a projection starts from.
	writeStartOffsetStatement = `INSERT INTO offsets_start (projection_name, start_offset) VALUES (?, ?)`
	// readStartOffsetStatement reads the timestamp a projection starts from.
	readStartOffsetStatement = `SELECT start_offset FROM offsets_start WHERE projection_name = ?`
	// deleteStartOffsetStatement removes the timestamp a projection starts from.
	deleteStartOffsetStatement = `DELETE FROM offsets_start WHERE projection_name = ?`
)

// OffsetStore persists the offsets of projections in the offsets_store,
// offsets_by_entity and offsets_start tables of
// resources/offsetstore_cassandra.cql. The offsets of a projection on a shard
// share one partition of each offsets table.
//
// WriteOffsets writes the offsets with logged batches of offsetsPerBatch
// offsets, the oldest timestamp first. Every batch is atomic. When a write
// fails or the process stops between two batches, the offsets stored are the
// oldest ones, so a projection's position on a shard, the newest timestamp it
// stored there, never passes an offset that was not stored, and the projection
// skips no event. Rewriting an offset replaces its rows with the same key.
//
// LatestOffsets reads the newest offset of every one of the
// persistence.NumberOfShards shards, so it runs one small query per shard;
// ResetOffset and DeleteOffset run the same queries to find the partitions to
// delete. The store has no transactional write: Cassandra has no
// multi-statement transactions, so it does not support
// projection.TransactionalHandler.
//
// Read and write with QUORUM or LOCAL_QUORUM, so that a projection restarting
// on another node reads the offsets it wrote. An OffsetStore is safe for
// concurrent use.
type OffsetStore struct {
	// session runs the store's statements. The application owns it.
	session *gocql.Session
}

// enforce interface implementation
var _ offsetstore.OffsetStore = (*OffsetStore)(nil)

// NewOffsetStore creates an offset store on the given session.
//
// The session stays owned by the caller: the store never closes it, so the same
// session can serve the other stores and the application's own queries.
func NewOffsetStore(session *gocql.Session) *OffsetStore {
	return &OffsetStore{session: session}
}

// Connect verifies the session reaches the cluster. The session is connected
// already when the application creates it.
func (s *OffsetStore) Connect(ctx context.Context) error {
	return ping(ctx, s.session)
}

// Disconnect releases nothing: the session belongs to the application, which
// closes it.
func (s *OffsetStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the cluster answers a query on the session.
func (s *OffsetStore) Ping(ctx context.Context) error {
	return ping(ctx, s.session)
}

// WriteOffsets stores the offsets of the projection, the oldest timestamp
// first, in logged batches of offsetsPerBatch offsets. An offset written again
// keeps a single row, which takes the consumption time of the latest write: the
// offset can only be kept longer by DeleteOffsetsBefore, never deleted sooner.
func (s *OffsetStore) WriteOffsets(ctx context.Context, projectionName string, offsets []*egopb.EntityOffset) error {
	ordered := slices.Clone(offsets)
	slices.SortStableFunc(ordered, func(left, right *egopb.EntityOffset) int {
		return cmp.Compare(left.GetTimestamp(), right.GetTimestamp())
	})

	for chunk := range slices.Chunk(ordered, offsetsPerBatch) {
		batch := s.session.Batch(gocql.LoggedBatch)
		for _, offset := range chunk {
			batch.Query(insertOffsetStatement, projectionName, offset.GetShardNumber(), offset.GetTimestamp(),
				offset.GetPersistenceId(), offset.GetSequenceNumber(), offset.GetConsumedAt())
			batch.Query(insertEntityOffsetStatement, projectionName, offset.GetShardNumber(), offset.GetPersistenceId(),
				offset.GetSequenceNumber(), offset.GetTimestamp(), offset.GetConsumedAt())
		}

		if err := batch.ExecContext(ctx); err != nil {
			return fmt.Errorf("cassandra: failed to write the offsets: %w", err)
		}
	}

	return nil
}

// ReadOffsets returns at most limit offsets of the projection on the shard,
// the newest timestamp first.
func (s *OffsetStore) ReadOffsets(ctx context.Context, projectionName string, shardNumber uint64, limit uint64) ([]*egopb.EntityOffset, error) {
	offsets := make([]*egopb.EntityOffset, 0, min(limit, maxPreallocatedRows))
	if limit == 0 {
		return offsets, nil
	}

	scanner := s.session.Query(readOffsetsStatement, projectionName, shardNumber, int(min(limit, math.MaxInt32))).
		Idempotent(true).
		IterContext(ctx).
		Scanner()

	for scanner.Next() {
		offset := &egopb.EntityOffset{ShardNumber: shardNumber}
		if err := scanner.Scan(&offset.PersistenceId, &offset.SequenceNumber, &offset.Timestamp, &offset.ConsumedAt); err != nil {
			return nil, fmt.Errorf("cassandra: failed to read the offsets: %w", err)
		}

		offsets = append(offsets, offset)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("cassandra: failed to read the offsets: %w", err)
	}

	return offsets, nil
}

// ReadEntityOffset returns the offset of the persistence ID with the highest
// sequence number, or nil when the projection has none.
func (s *OffsetStore) ReadEntityOffset(ctx context.Context, projectionName string, shardNumber uint64, persistenceID string) (*egopb.EntityOffset, error) {
	offset := &egopb.EntityOffset{ShardNumber: shardNumber, PersistenceId: persistenceID}
	err := s.session.Query(readEntityOffsetStatement, projectionName, shardNumber, persistenceID).
		Idempotent(true).
		ScanContext(ctx, &offset.SequenceNumber, &offset.Timestamp, &offset.ConsumedAt)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("cassandra: failed to read the entity offset: %w", err)
	}

	return offset, nil
}

// LatestOffsets returns, for every shard the projection has an offset on, the
// newest timestamp among its offsets on that shard. It runs one query per shard,
// shardQueryConcurrency at a time.
func (s *OffsetStore) LatestOffsets(ctx context.Context, projectionName string) (map[uint64]int64, error) {
	latest, err := s.latestOffsets(ctx, projectionName)
	if err != nil {
		return nil, fmt.Errorf("cassandra: failed to read the latest offsets: %w", err)
	}

	return latest, nil
}

// DeleteOffsetsBefore deletes the offsets of the projection on the shard whose
// timestamp is before before and that were consumed before consumedBefore,
// both in Unix nanoseconds, from both offsets tables.
func (s *OffsetStore) DeleteOffsetsBefore(ctx context.Context, projectionName string, shardNumber uint64, before, consumedBefore int64) error {
	scanner := s.session.Query(offsetsBeforeStatement, projectionName, shardNumber, before).
		Idempotent(true).
		IterContext(ctx).
		Scanner()

	var expired []*egopb.EntityOffset
	for scanner.Next() {
		offset := &egopb.EntityOffset{ShardNumber: shardNumber}
		if err := scanner.Scan(&offset.Timestamp, &offset.PersistenceId, &offset.SequenceNumber, &offset.ConsumedAt); err != nil {
			return fmt.Errorf("cassandra: failed to delete the offsets: %w", err)
		}

		if offset.GetConsumedAt() < consumedBefore {
			expired = append(expired, offset)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("cassandra: failed to delete the offsets: %w", err)
	}

	for chunk := range slices.Chunk(expired, offsetsPerBatch) {
		batch := s.session.Batch(gocql.LoggedBatch)
		for _, offset := range chunk {
			batch.Query(deleteOffsetStatement, projectionName, shardNumber, offset.GetTimestamp(), offset.GetPersistenceId(), offset.GetSequenceNumber())
			batch.Query(deleteEntityOffsetStatement, projectionName, shardNumber, offset.GetPersistenceId(), offset.GetSequenceNumber())
		}

		if err := batch.ExecContext(ctx); err != nil {
			return fmt.Errorf("cassandra: failed to delete the offsets: %w", err)
		}
	}

	return nil
}

// ResetOffset deletes every offset of the projection, then records value, a
// timestamp in Unix nanoseconds, as the point it starts from on every shard.
func (s *OffsetStore) ResetOffset(ctx context.Context, projectionName string, value int64) error {
	if err := s.deleteOffsets(ctx, projectionName); err != nil {
		return fmt.Errorf("cassandra: failed to reset the offsets: %w", err)
	}

	if err := s.session.Query(writeStartOffsetStatement, projectionName, value).Idempotent(true).ExecContext(ctx); err != nil {
		return fmt.Errorf("cassandra: failed to reset the offsets: %w", err)
	}

	return nil
}

// StartOffset returns the timestamp the latest ResetOffset recorded for the
// projection, or zero when there is none.
func (s *OffsetStore) StartOffset(ctx context.Context, projectionName string) (int64, error) {
	var start int64
	err := s.session.Query(readStartOffsetStatement, projectionName).Idempotent(true).ScanContext(ctx, &start)
	if errors.Is(err, gocql.ErrNotFound) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("cassandra: failed to read the start offset: %w", err)
	}

	return start, nil
}

// DeleteOffset deletes every offset and the start offset of the projection. A
// projection with no offset is not an error.
func (s *OffsetStore) DeleteOffset(ctx context.Context, projectionName string) error {
	if err := s.deleteOffsets(ctx, projectionName); err != nil {
		return fmt.Errorf("cassandra: failed to delete the offsets: %w", err)
	}

	if err := s.session.Query(deleteStartOffsetStatement, projectionName).Idempotent(true).ExecContext(ctx); err != nil {
		return fmt.Errorf("cassandra: failed to delete the offsets: %w", err)
	}

	return nil
}

// latestOffsets reads the newest timestamp of the projection on every shard,
// shardQueryConcurrency queries at a time, and keeps the shards that have one.
func (s *OffsetStore) latestOffsets(ctx context.Context, projectionName string) (map[uint64]int64, error) {
	var (
		timestamps [persistence.NumberOfShards]int64
		found      [persistence.NumberOfShards]bool
	)

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(shardQueryConcurrency)

	for shardNumber := range persistence.NumberOfShards {
		group.Go(func() error {
			err := s.session.Query(latestOffsetStatement, projectionName, shardNumber).
				Idempotent(true).
				ScanContext(groupCtx, &timestamps[shardNumber])
			if errors.Is(err, gocql.ErrNotFound) {
				return nil
			}

			found[shardNumber] = err == nil
			return err
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	latest := make(map[uint64]int64)
	for shardNumber, ok := range found {
		if ok {
			latest[uint64(shardNumber)] = timestamps[shardNumber]
		}
	}

	return latest, nil
}

// deleteOffsets deletes the partitions of both offsets tables that hold
// offsets of the projection, shardQueryConcurrency shards at a time.
func (s *OffsetStore) deleteOffsets(ctx context.Context, projectionName string) error {
	latest, err := s.latestOffsets(ctx, projectionName)
	if err != nil {
		return err
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(shardQueryConcurrency)

	for shardNumber := range latest {
		group.Go(func() error {
			return s.session.Batch(gocql.LoggedBatch).
				Query(deleteShardOffsetsStatement, projectionName, shardNumber).
				Query(deleteShardEntityOffsetsStatement, projectionName, shardNumber).
				ExecContext(groupCtx)
		})
	}

	return group.Wait()
}
