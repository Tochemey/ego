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

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/offsetstore"
)

const (
	// offsetColumnCount is the number of columns an offset row binds on insert.
	offsetColumnCount = 6
	// offsetColumns are the columns an offset is read from, in the order scanOffset reads them.
	offsetColumns = "shard_number, persistence_id, sequence_number, timestamp, consumed_at"

	// readOffsetsQuery reads a projection's offsets on a shard, newest first, through the primary key.
	readOffsetsQuery = "SELECT " + offsetColumns + " FROM offsets_store " +
		"WHERE projection_name = ? AND shard_number = ? " +
		"ORDER BY timestamp DESC, persistence_id DESC, sequence_number DESC LIMIT ?"
	// readEntityOffsetQuery reads an entity's offset with the highest sequence number through idx_offsets_store_entity.
	readEntityOffsetQuery = "SELECT " + offsetColumns + " FROM offsets_store " +
		"WHERE projection_name = ? AND shard_number = ? AND persistence_id = ? " +
		"ORDER BY sequence_number DESC LIMIT 1"
	// latestOffsetsQuery reads the newest timestamp of a projection on each of its shards through the primary key.
	latestOffsetsQuery = "SELECT shard_number, MAX(timestamp) FROM offsets_store " +
		"WHERE projection_name = ? GROUP BY shard_number"
	// deleteOffsetsBeforeQuery deletes a projection's old offsets on a shard through the primary key.
	deleteOffsetsBeforeQuery = "DELETE FROM offsets_store " +
		"WHERE projection_name = ? AND shard_number = ? AND timestamp < ? AND consumed_at < ?"
	// deleteOffsetsQuery deletes every offset of a projection through the primary key.
	deleteOffsetsQuery = "DELETE FROM offsets_store WHERE projection_name = ?"
	// writeStartOffsetQuery records the timestamp a projection starts from.
	writeStartOffsetQuery = "INSERT INTO offsets_start (projection_name, start_offset) VALUES (?, ?) " +
		"ON CONFLICT (projection_name) DO UPDATE SET start_offset = excluded.start_offset"
	// readStartOffsetQuery reads the timestamp a projection starts from.
	readStartOffsetQuery = "SELECT start_offset FROM offsets_start WHERE projection_name = ?"
	// deleteStartOffsetQuery deletes the timestamp a projection starts from.
	deleteStartOffsetQuery = "DELETE FROM offsets_start WHERE projection_name = ?"
)

// insertOffsets writes offsets, keeping an offset that is stored already.
var insertOffsets = insertStatement{
	prefix: "INSERT INTO offsets_store " +
		"(projection_name, shard_number, timestamp, persistence_id, sequence_number, consumed_at) VALUES ",
	columns: offsetColumnCount,
	suffix:  " ON CONFLICT DO NOTHING",
}

// OffsetStore implements offsetstore.OffsetStore on the offsets_store and offsets_start
// tables of a SQLite database.
type OffsetStore struct {
	// db is the shared database handle, owned by the caller.
	db *sql.DB
}

// enforce interface implementation
var _ offsetstore.OffsetStore = (*OffsetStore)(nil)

// NewOffsetStore creates an offset store running on db, a handle opened with NewDB.
// The store never closes db.
func NewOffsetStore(db *sql.DB) *OffsetStore {
	return &OffsetStore{db: db}
}

// Connect verifies the database is reachable.
func (s *OffsetStore) Connect(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Disconnect releases nothing: the database handle stays open for its owner to close.
func (s *OffsetStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the database is reachable.
func (s *OffsetStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// WriteOffsets stores the offsets of the projection atomically. An offset that is stored
// already is kept as is.
func (s *OffsetStore) WriteOffsets(ctx context.Context, projectionName string, offsets []*egopb.EntityOffset) error {
	if err := insertOffsets.write(ctx, s.db, offsetArgs(projectionName, offsets)); err != nil {
		return fmt.Errorf("sqlite: failed to write the offsets: %w", err)
	}

	return nil
}

// WriteOffsetsTx stores the offsets of the projection within tx, a transaction the caller
// began on the store's database handle and commits or rolls back. A
// projection.TransactionalHandler calls it to record an event's offset in the transaction
// that applies the event, so both are committed together.
func (s *OffsetStore) WriteOffsetsTx(ctx context.Context, tx *sql.Tx, projectionName string, offsets []*egopb.EntityOffset) error {
	if err := insertOffsets.exec(ctx, tx, offsetArgs(projectionName, offsets)); err != nil {
		return fmt.Errorf("sqlite: failed to write the offsets: %w", err)
	}

	return nil
}

// ReadOffsets returns at most limit offsets of the projection on the shard, the newest timestamp first.
func (s *OffsetStore) ReadOffsets(ctx context.Context, projectionName string, shardNumber uint64, limit uint64) ([]*egopb.EntityOffset, error) {
	rows, err := s.db.QueryContext(ctx, readOffsetsQuery, projectionName, shardNumber, limit)
	offsets, err := collect(rows, err, limit, scanOffset)
	if err != nil {
		return nil, fmt.Errorf("sqlite: failed to read the offsets: %w", err)
	}

	return offsets, nil
}

// ReadEntityOffset returns the offset of the persistence ID with the highest sequence number,
// or nil when the projection has no offset for it.
func (s *OffsetStore) ReadEntityOffset(ctx context.Context, projectionName string, shardNumber uint64, persistenceID string) (*egopb.EntityOffset, error) {
	offset, err := scanOffset(s.db.QueryRowContext(ctx, readEntityOffsetQuery, projectionName, shardNumber, persistenceID))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("sqlite: failed to read the entity offset: %w", err)
	default:
		return offset, nil
	}
}

// LatestOffsets returns, for every shard the projection has an offset on, the newest timestamp
// among its offsets on that shard.
func (s *OffsetStore) LatestOffsets(ctx context.Context, projectionName string) (map[uint64]int64, error) {
	rows, err := s.db.QueryContext(ctx, latestOffsetsQuery, projectionName)
	offsets, err := collectShardTimestamps(rows, err)
	if err != nil {
		return nil, fmt.Errorf("sqlite: failed to read the latest offsets: %w", err)
	}

	return offsets, nil
}

// DeleteOffsetsBefore deletes the offsets of the projection on the shard whose timestamp is
// before the given one and that were consumed before consumedBefore.
func (s *OffsetStore) DeleteOffsetsBefore(ctx context.Context, projectionName string, shardNumber uint64, before, consumedBefore int64) error {
	if _, err := s.db.ExecContext(ctx, deleteOffsetsBeforeQuery, projectionName, shardNumber, before, consumedBefore); err != nil {
		return fmt.Errorf("sqlite: failed to delete the offsets: %w", err)
	}

	return nil
}

// ResetOffset deletes every offset of the projection and records value as the timestamp it
// starts from, in one transaction.
func (s *OffsetStore) ResetOffset(ctx context.Context, projectionName string, value int64) error {
	if err := s.inTransaction(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, deleteOffsetsQuery, projectionName); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx, writeStartOffsetQuery, projectionName, value)
		return err
	}); err != nil {
		return fmt.Errorf("sqlite: failed to reset the offsets: %w", err)
	}

	return nil
}

// StartOffset returns the timestamp the latest ResetOffset recorded for the projection,
// or zero when there is none.
func (s *OffsetStore) StartOffset(ctx context.Context, projectionName string) (int64, error) {
	var start int64
	err := s.db.QueryRowContext(ctx, readStartOffsetQuery, projectionName).Scan(&start)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("sqlite: failed to read the start offset: %w", err)
	}

	return start, nil
}

// DeleteOffset deletes every offset and the start offset of the projection, in one transaction.
func (s *OffsetStore) DeleteOffset(ctx context.Context, projectionName string) error {
	if err := s.inTransaction(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, deleteOffsetsQuery, projectionName); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx, deleteStartOffsetQuery, projectionName)
		return err
	}); err != nil {
		return fmt.Errorf("sqlite: failed to delete the offsets: %w", err)
	}

	return nil
}

// inTransaction runs statements within one transaction, committed when statements succeeds.
func (s *OffsetStore) inTransaction(ctx context.Context, statements func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback() }()

	if err := statements(tx); err != nil {
		return err
	}

	return tx.Commit()
}

// offsetArgs lists the column values of the projection's offsets, row after row, as insertOffsets binds them.
func offsetArgs(projectionName string, offsets []*egopb.EntityOffset) []any {
	args := make([]any, 0, len(offsets)*offsetColumnCount)
	for _, offset := range offsets {
		args = append(args,
			projectionName,
			offset.GetShardNumber(),
			offset.GetTimestamp(),
			offset.GetPersistenceId(),
			offset.GetSequenceNumber(),
			offset.GetConsumedAt(),
		)
	}

	return args
}

// scanOffset reads an offset from a row of offsetColumns.
func scanOffset(row scanner) (*egopb.EntityOffset, error) {
	offset := new(egopb.EntityOffset)
	err := row.Scan(
		&offset.ShardNumber,
		&offset.PersistenceId,
		&offset.SequenceNumber,
		&offset.Timestamp,
		&offset.ConsumedAt,
	)

	return offset, err
}
