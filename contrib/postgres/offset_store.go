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

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/offsetstore"
)

const (
	// insertOffsetsSQL inserts any number of offsets of a projection in one
	// statement, one array per column. An offset stored already is kept once.
	insertOffsetsSQL = "INSERT INTO offsets_store " +
		"(projection_name, shard_number, timestamp, persistence_id, sequence_number, consumed_at) " +
		"SELECT $1::text, * FROM unnest($2::bigint[], $3::bigint[], $4::text[], $5::bigint[], $6::bigint[]) " +
		"ON CONFLICT DO NOTHING"

	// readOffsetsSQL reads the newest offsets first; the persistence ID and the
	// sequence number order the offsets that share a timestamp, so a page is the
	// same every time. The primary key, read backwards, serves the order.
	readOffsetsSQL = "SELECT persistence_id, sequence_number, timestamp, consumed_at FROM offsets_store " +
		"WHERE projection_name = $1 AND shard_number = $2 " +
		"ORDER BY timestamp DESC, persistence_id DESC, sequence_number DESC LIMIT $3"

	readEntityOffsetSQL = "SELECT sequence_number, timestamp, consumed_at FROM offsets_store " +
		"WHERE projection_name = $1 AND shard_number = $2 AND persistence_id = $3 " +
		"ORDER BY sequence_number DESC LIMIT 1"

	latestOffsetsSQL = "SELECT shard_number, MAX(timestamp) FROM offsets_store " +
		"WHERE projection_name = $1 GROUP BY shard_number"

	deleteOffsetsBeforeSQL = "DELETE FROM offsets_store " +
		"WHERE projection_name = $1 AND shard_number = $2 AND timestamp < $3 AND consumed_at < $4"

	// resetOffsetSQL deletes the offsets of a projection and records its start
	// offset in one statement.
	resetOffsetSQL = "WITH deleted AS (DELETE FROM offsets_store WHERE projection_name = $1) " +
		"INSERT INTO offsets_start (projection_name, start_offset) VALUES ($1, $2) " +
		"ON CONFLICT (projection_name) DO UPDATE SET start_offset = EXCLUDED.start_offset"

	startOffsetSQL = "SELECT start_offset FROM offsets_start WHERE projection_name = $1"

	// deleteOffsetSQL deletes the offsets and the start offset of a projection
	// in one statement.
	deleteOffsetSQL = "WITH deleted AS (DELETE FROM offsets_store WHERE projection_name = $1) " +
		"DELETE FROM offsets_start WHERE projection_name = $1"
)

// OffsetStore implements offsetstore.OffsetStore on the offsets_store and
// offsets_start tables of resources/offsetstore_postgres.sql.
type OffsetStore struct {
	// pool runs the statements. The application owns it.
	pool Pool
}

// enforce the interface implementation
var _ offsetstore.OffsetStore = (*OffsetStore)(nil)

// NewOffsetStore creates an offset store running on the given pool. The store
// never closes the pool, so the pool can be shared with the other stores and
// with the application, for instance with a projection.TransactionalHandler
// that records offsets with WriteOffsetsTx.
func NewOffsetStore(pool Pool) *OffsetStore {
	return &OffsetStore{pool: pool}
}

// Connect verifies the database is reachable.
func (s *OffsetStore) Connect(ctx context.Context) error {
	return connect(ctx, s.pool)
}

// Disconnect releases nothing: the pool belongs to the application.
func (s *OffsetStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the database is reachable.
func (s *OffsetStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// WriteOffsets stores the offsets of the projection in one statement, so
// either every offset is stored or none is. An offset stored already is kept once.
func (s *OffsetStore) WriteOffsets(ctx context.Context, projectionName string, offsets []*egopb.EntityOffset) error {
	return writeOffsets(ctx, s.pool, projectionName, offsets)
}

// WriteOffsetsTx stores the offsets of the projection within the caller's
// transaction, with the same statement as WriteOffsets.
//
// A projection.TransactionalHandler calls it in the transaction that applies
// the event, so the effect and the offset commit together. The transaction
// must run on the database of the store; the caller commits or rolls it back.
func (s *OffsetStore) WriteOffsetsTx(ctx context.Context, tx pgx.Tx, projectionName string, offsets []*egopb.EntityOffset) error {
	return writeOffsets(ctx, tx, projectionName, offsets)
}

// ReadOffsets returns at most limit offsets of the projection on the shard,
// the newest timestamp first.
func (s *OffsetStore) ReadOffsets(ctx context.Context, projectionName string, shardNumber uint64, limit uint64) ([]*egopb.EntityOffset, error) {
	rows, err := s.pool.Query(ctx, readOffsetsSQL, projectionName, int64(shardNumber), int64(limit))
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the offsets: %w", err)
	}

	offsets, err := pgx.AppendRows(make([]*egopb.EntityOffset, 0, capacity(limit)), rows, func(row pgx.CollectableRow) (*egopb.EntityOffset, error) {
		offset := &egopb.EntityOffset{ShardNumber: shardNumber}
		err := row.Scan(&offset.PersistenceId, &offset.SequenceNumber, &offset.Timestamp, &offset.ConsumedAt)
		return offset, err
	})

	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the offsets: %w", err)
	}

	return offsets, nil
}

// ReadEntityOffset returns the offset of the persistence ID with the highest
// sequence number, or nil when the projection has none.
func (s *OffsetStore) ReadEntityOffset(ctx context.Context, projectionName string, shardNumber uint64, persistenceID string) (*egopb.EntityOffset, error) {
	offset := &egopb.EntityOffset{ShardNumber: shardNumber, PersistenceId: persistenceID}
	err := s.pool.QueryRow(ctx, readEntityOffsetSQL, projectionName, int64(shardNumber), persistenceID).
		Scan(&offset.SequenceNumber, &offset.Timestamp, &offset.ConsumedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the entity offset: %w", err)
	}

	return offset, nil
}

// LatestOffsets returns, for every shard the projection has an offset on, the
// newest timestamp among its offsets on that shard.
func (s *OffsetStore) LatestOffsets(ctx context.Context, projectionName string) (map[uint64]int64, error) {
	rows, err := s.pool.Query(ctx, latestOffsetsSQL, projectionName)
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the latest offsets: %w", err)
	}

	latest, err := collectTimestamps(rows)
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the latest offsets: %w", err)
	}

	return latest, nil
}

// DeleteOffsetsBefore deletes the offsets of the projection on the shard whose
// timestamp is before before and that were consumed before consumedBefore,
// both in Unix nanoseconds.
func (s *OffsetStore) DeleteOffsetsBefore(ctx context.Context, projectionName string, shardNumber uint64, before, consumedBefore int64) error {
	if _, err := s.pool.Exec(ctx, deleteOffsetsBeforeSQL, projectionName, int64(shardNumber), before, consumedBefore); err != nil {
		return fmt.Errorf("postgres: failed to delete the offsets: %w", err)
	}

	return nil
}

// ResetOffset deletes every offset of the projection and records value, in
// Unix nanoseconds, as the point it starts from on every shard, in one statement.
func (s *OffsetStore) ResetOffset(ctx context.Context, projectionName string, value int64) error {
	if _, err := s.pool.Exec(ctx, resetOffsetSQL, projectionName, value); err != nil {
		return fmt.Errorf("postgres: failed to reset the offsets: %w", err)
	}

	return nil
}

// StartOffset returns the start offset the latest ResetOffset recorded for the
// projection, or zero when there is none.
func (s *OffsetStore) StartOffset(ctx context.Context, projectionName string) (int64, error) {
	var start int64
	err := s.pool.QueryRow(ctx, startOffsetSQL, projectionName).Scan(&start)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("postgres: failed to read the start offset: %w", err)
	}

	return start, nil
}

// DeleteOffset deletes every offset and the start offset of the projection in
// one statement. A projection with no offset is not an error.
func (s *OffsetStore) DeleteOffset(ctx context.Context, projectionName string) error {
	if _, err := s.pool.Exec(ctx, deleteOffsetSQL, projectionName); err != nil {
		return fmt.Errorf("postgres: failed to delete the offsets: %w", err)
	}

	return nil
}

// executor runs a statement that returns no rows: a Pool or a pgx.Tx.
type executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// writeOffsets runs insertOffsetsSQL for the offsets on the given executor.
func writeOffsets(ctx context.Context, exec executor, projectionName string, offsets []*egopb.EntityOffset) error {
	if len(offsets) == 0 {
		return nil
	}

	var (
		shardNumbers    = make([]int64, len(offsets))
		timestamps      = make([]int64, len(offsets))
		persistenceIDs  = make([]string, len(offsets))
		sequenceNumbers = make([]int64, len(offsets))
		consumedAt      = make([]int64, len(offsets))
	)

	for index, offset := range offsets {
		shardNumbers[index] = int64(offset.GetShardNumber())
		timestamps[index] = offset.GetTimestamp()
		persistenceIDs[index] = offset.GetPersistenceId()
		sequenceNumbers[index] = int64(offset.GetSequenceNumber())
		consumedAt[index] = offset.GetConsumedAt()
	}

	if _, err := exec.Exec(ctx, insertOffsetsSQL,
		projectionName, shardNumbers, timestamps, persistenceIDs, sequenceNumbers, consumedAt); err != nil {
		return fmt.Errorf("postgres: failed to write the offsets: %w", err)
	}

	return nil
}
