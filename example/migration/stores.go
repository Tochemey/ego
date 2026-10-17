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

package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/migration"
	"github.com/tochemey/ego/v5/offsetstore"
	"github.com/tochemey/ego/v5/persistence"
)

// eventColumns lists the columns of events_store in the order the queries read them.
const eventColumns = `persistence_id, sequence_number, is_deleted, event_payload, event_manifest,
	timestamp, shard_number, encryption_key_id, is_encrypted`

// eventsStore is a persistence.EventsStore over the events_store table. The
// table has the same columns in a v4 and a v5 database, so the same store reads
// the legacy journal and writes the migrated one.
type eventsStore struct {
	// pool is the connection pool of the database holding the table.
	pool *pgxpool.Pool
}

var (
	_ persistence.EventsStore = (*eventsStore)(nil)
	_ migration.JournalReader = (*eventsStore)(nil)
)

// Connect does nothing: the store is created over an open pool.
func (s *eventsStore) Connect(context.Context) error { return nil }

// Disconnect does nothing: the pool's owner closes it.
func (s *eventsStore) Disconnect(context.Context) error { return nil }

// Ping verifies the database is reachable.
func (s *eventsStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// WriteEvents inserts the events in one transaction, sent as one batch. A
// plain insert makes the primary key reject an event already stored, which
// fails the whole write, as the events store contract requires.
func (s *eventsStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, event := range events {
		payload, err := proto.Marshal(event.GetEvent())
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		batch.Queue(`INSERT INTO events_store (`+eventColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			event.GetPersistenceId(),
			event.GetSequenceNumber(),
			event.GetIsDeleted(),
			payload,
			event.GetEvent().GetTypeUrl(),
			event.GetTimestamp(),
			event.GetShard(),
			event.GetEncryptionKeyId(),
			event.GetIsEncrypted(),
		)
	}

	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("insert events: %w", err)
	}
	return tx.Commit(ctx)
}

// DeleteEvents deletes the events of the persistence ID up to the sequence number.
func (s *eventsStore) DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM events_store WHERE persistence_id=$1 AND sequence_number<=$2`,
		persistenceID, toSequenceNumber)
	return err
}

// ReplayEvents returns at most limit events of the persistence ID between the two sequence numbers.
func (s *eventsStore) ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber, limit uint64) ([]*egopb.Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+eventColumns+` FROM events_store
		WHERE persistence_id=$1 AND sequence_number>=$2 AND sequence_number<=$3
		ORDER BY sequence_number LIMIT $4`,
		persistenceID, fromSequenceNumber, toSequenceNumber, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

// GetLatestEvent returns the latest event of the persistence ID, or nil.
func (s *eventsStore) GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+eventColumns+` FROM events_store
		WHERE persistence_id=$1 ORDER BY sequence_number DESC LIMIT 1`, persistenceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events, err := scanEvents(rows)
	if err != nil || len(events) == 0 {
		return nil, err
	}
	return events[0], nil
}

// PersistenceIDs returns a page of the persistence IDs of the journal, sorted,
// starting after pageToken, the last persistence ID of the previous page.
func (s *eventsStore) PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) ([]string, string, error) {
	return pagePersistenceIDs(ctx, s.pool, `SELECT DISTINCT persistence_id FROM events_store
		WHERE persistence_id > $1 ORDER BY persistence_id LIMIT $2`, pageSize, pageToken)
}

// EventsByShard returns at most limit events of the shard stamped at or after
// fromTimestamp, ordered by timestamp, sequence number and persistence ID.
func (s *eventsStore) EventsByShard(ctx context.Context, shardNumber uint64, fromTimestamp int64, limit uint64) ([]*egopb.Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+eventColumns+` FROM events_store
		WHERE shard_number=$1 AND timestamp >= $2
		ORDER BY timestamp, sequence_number, persistence_id LIMIT $3`,
		shardNumber, fromTimestamp, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

// ShardOffsets returns the newest event timestamp of every shard.
func (s *eventsStore) ShardOffsets(ctx context.Context) (map[uint64]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT shard_number, MAX(timestamp) FROM events_store GROUP BY shard_number`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	offsets := make(map[uint64]int64)
	for rows.Next() {
		var (
			shard  uint64
			newest int64
		)
		if err := rows.Scan(&shard, &newest); err != nil {
			return nil, err
		}
		offsets[shard] = newest
	}
	return offsets, rows.Err()
}

// snapshotsStore is a persistence.SnapshotStore over the snapshots_store
// table that also lists its persistence IDs, as migration.SnapshotReader needs.
type snapshotsStore struct {
	// pool is the connection pool of the database holding the table.
	pool *pgxpool.Pool
}

var (
	_ persistence.SnapshotStore = (*snapshotsStore)(nil)
	_ migration.SnapshotReader  = (*snapshotsStore)(nil)
)

// Connect does nothing: the store is created over an open pool.
func (s *snapshotsStore) Connect(context.Context) error { return nil }

// Disconnect does nothing: the pool's owner closes it.
func (s *snapshotsStore) Disconnect(context.Context) error { return nil }

// Ping verifies the database is reachable.
func (s *snapshotsStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// WriteSnapshot stores the snapshot; a snapshot already stored is kept.
func (s *snapshotsStore) WriteSnapshot(ctx context.Context, snapshot *egopb.Snapshot) error {
	payload, err := proto.Marshal(snapshot.GetState())
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	_, err = s.pool.Exec(ctx, `INSERT INTO snapshots_store
		(persistence_id, sequence_number, state_payload, state_manifest, timestamp, encryption_key_id, is_encrypted)
		VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`,
		snapshot.GetPersistenceId(),
		snapshot.GetSequenceNumber(),
		payload,
		snapshot.GetState().GetTypeUrl(),
		snapshot.GetTimestamp(),
		snapshot.GetEncryptionKeyId(),
		snapshot.GetIsEncrypted(),
	)
	return err
}

// GetLatestSnapshot returns the snapshot of the persistence ID with the highest sequence number, or nil.
func (s *snapshotsStore) GetLatestSnapshot(ctx context.Context, persistenceID string) (*egopb.Snapshot, error) {
	var (
		sequenceNumber  uint64
		payload         []byte
		manifest        string
		timestamp       int64
		encryptionKeyID string
		isEncrypted     bool
	)

	err := s.pool.QueryRow(ctx, `SELECT sequence_number, state_payload, state_manifest, timestamp, encryption_key_id, is_encrypted
		FROM snapshots_store WHERE persistence_id=$1 ORDER BY sequence_number DESC LIMIT 1`, persistenceID).
		Scan(&sequenceNumber, &payload, &manifest, &timestamp, &encryptionKeyID, &isEncrypted)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	state := &anypb.Any{TypeUrl: manifest}
	if err := proto.Unmarshal(payload, state); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}

	return &egopb.Snapshot{
		PersistenceId:   persistenceID,
		SequenceNumber:  sequenceNumber,
		State:           state,
		Timestamp:       timestamp,
		EncryptionKeyId: encryptionKeyID,
		IsEncrypted:     isEncrypted,
	}, nil
}

// DeleteSnapshots deletes the snapshots of the persistence ID up to the sequence number.
func (s *snapshotsStore) DeleteSnapshots(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM snapshots_store WHERE persistence_id=$1 AND sequence_number<=$2`,
		persistenceID, toSequenceNumber)
	return err
}

// PersistenceIDs returns a page of the persistence IDs that have a snapshot,
// sorted, starting after pageToken.
func (s *snapshotsStore) PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) ([]string, string, error) {
	return pagePersistenceIDs(ctx, s.pool, `SELECT DISTINCT persistence_id FROM snapshots_store
		WHERE persistence_id > $1 ORDER BY persistence_id LIMIT $2`, pageSize, pageToken)
}

// legacyOffsets reads the v4 offsets_store table, one offset per projection and shard.
type legacyOffsets struct {
	// pool is the connection pool of the legacy database.
	pool *pgxpool.Pool
}

var _ migration.LegacyOffsetReader = (*legacyOffsets)(nil)

// LegacyOffsets returns the offsets every projection committed, one per legacy shard.
func (s *legacyOffsets) LegacyOffsets(ctx context.Context) (map[string][]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT projection_name, current_offset FROM offsets_store`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	offsets := make(map[string][]int64)
	for rows.Next() {
		var (
			projectionName string
			offset         int64
		)
		if err := rows.Scan(&projectionName, &offset); err != nil {
			return nil, err
		}
		offsets[projectionName] = append(offsets[projectionName], offset)
	}
	return offsets, rows.Err()
}

// offsetsStore is a v5 offsetstore.OffsetStore over the offsets_store and
// offsets_start tables, one row per entity a projection processed.
type offsetsStore struct {
	// pool is the connection pool of the database holding the tables.
	pool *pgxpool.Pool
}

var _ offsetstore.OffsetStore = (*offsetsStore)(nil)

// Connect does nothing: the store is created over an open pool.
func (s *offsetsStore) Connect(context.Context) error { return nil }

// Disconnect does nothing: the pool's owner closes it.
func (s *offsetsStore) Disconnect(context.Context) error { return nil }

// Ping verifies the database is reachable.
func (s *offsetsStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// WriteOffsets inserts the offsets in one transaction, sent as one batch.
func (s *offsetsStore) WriteOffsets(ctx context.Context, projectionName string, offsets []*egopb.EntityOffset) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, offset := range offsets {
		batch.Queue(`INSERT INTO offsets_store
			(projection_name, shard_number, timestamp, persistence_id, sequence_number, consumed_at)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`,
			projectionName, offset.GetShardNumber(), offset.GetTimestamp(),
			offset.GetPersistenceId(), offset.GetSequenceNumber(), offset.GetConsumedAt())
	}

	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("insert offsets: %w", err)
	}
	return tx.Commit(ctx)
}

// ReadOffsets returns at most limit offsets of the projection on the shard, newest first.
func (s *offsetsStore) ReadOffsets(ctx context.Context, projectionName string, shardNumber uint64, limit uint64) ([]*egopb.EntityOffset, error) {
	rows, err := s.pool.Query(ctx, `SELECT shard_number, persistence_id, sequence_number, timestamp, consumed_at
		FROM offsets_store WHERE projection_name=$1 AND shard_number=$2
		ORDER BY timestamp DESC LIMIT $3`, projectionName, shardNumber, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOffsets(rows)
}

// ReadEntityOffset returns the offset of the persistence ID with the highest sequence number, or nil.
func (s *offsetsStore) ReadEntityOffset(ctx context.Context, projectionName string, shardNumber uint64, persistenceID string) (*egopb.EntityOffset, error) {
	rows, err := s.pool.Query(ctx, `SELECT shard_number, persistence_id, sequence_number, timestamp, consumed_at
		FROM offsets_store WHERE projection_name=$1 AND shard_number=$2 AND persistence_id=$3
		ORDER BY sequence_number DESC LIMIT 1`, projectionName, shardNumber, persistenceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	offsets, err := scanOffsets(rows)
	if err != nil || len(offsets) == 0 {
		return nil, err
	}
	return offsets[0], nil
}

// LatestOffsets returns the newest offset timestamp of the projection on every shard.
func (s *offsetsStore) LatestOffsets(ctx context.Context, projectionName string) (map[uint64]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT shard_number, MAX(timestamp) FROM offsets_store
		WHERE projection_name=$1 GROUP BY shard_number`, projectionName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	latest := make(map[uint64]int64)
	for rows.Next() {
		var (
			shard  uint64
			newest int64
		)
		if err := rows.Scan(&shard, &newest); err != nil {
			return nil, err
		}
		latest[shard] = newest
	}
	return latest, rows.Err()
}

// DeleteOffsetsBefore deletes the projection's offsets on the shard older than both bounds.
func (s *offsetsStore) DeleteOffsetsBefore(ctx context.Context, projectionName string, shardNumber uint64, before, consumedBefore int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM offsets_store
		WHERE projection_name=$1 AND shard_number=$2 AND timestamp < $3 AND consumed_at < $4`,
		projectionName, shardNumber, before, consumedBefore)
	return err
}

// ResetOffset deletes every offset of the projection and records value as its start offset.
func (s *offsetsStore) ResetOffset(ctx context.Context, projectionName string, value int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM offsets_store WHERE projection_name=$1`, projectionName); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `INSERT INTO offsets_start (projection_name, start_offset) VALUES ($1, $2)
		ON CONFLICT (projection_name) DO UPDATE SET start_offset = EXCLUDED.start_offset`, projectionName, value); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// StartOffset returns the start offset ResetOffset recorded for the projection, or zero.
func (s *offsetsStore) StartOffset(ctx context.Context, projectionName string) (int64, error) {
	var start int64
	err := s.pool.QueryRow(ctx, `SELECT start_offset FROM offsets_start WHERE projection_name=$1`, projectionName).Scan(&start)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return start, err
}

// DeleteOffset deletes every offset and the start offset of the projection.
func (s *offsetsStore) DeleteOffset(ctx context.Context, projectionName string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM offsets_store WHERE projection_name=$1`, projectionName); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM offsets_start WHERE projection_name=$1`, projectionName); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// pagePersistenceIDs runs a paging query whose parameters are the page token
// and the page size, and returns the page and the token of the next one: the
// page's last persistence ID, or empty after the last page.
func pagePersistenceIDs(ctx context.Context, pool *pgxpool.Pool, query string, pageSize uint64, pageToken string) ([]string, string, error) {
	rows, err := pool.Query(ctx, query, pageToken, pageSize)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	persistenceIDs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, "", err
	}

	nextPageToken := ""
	if uint64(len(persistenceIDs)) == pageSize {
		nextPageToken = persistenceIDs[len(persistenceIDs)-1]
	}
	return persistenceIDs, nextPageToken, nil
}

// scanEvents reads event rows in the column order of eventColumns.
func scanEvents(rows pgx.Rows) ([]*egopb.Event, error) {
	var events []*egopb.Event
	for rows.Next() {
		var (
			event    = new(egopb.Event)
			payload  []byte
			manifest string
		)
		if err := rows.Scan(&event.PersistenceId, &event.SequenceNumber, &event.IsDeleted, &payload, &manifest,
			&event.Timestamp, &event.Shard, &event.EncryptionKeyId, &event.IsEncrypted); err != nil {
			return nil, err
		}

		event.Event = &anypb.Any{TypeUrl: manifest}
		if err := proto.Unmarshal(payload, event.Event); err != nil {
			return nil, fmt.Errorf("unmarshal event payload: %w", err)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

// scanOffsets reads offset rows.
func scanOffsets(rows pgx.Rows) ([]*egopb.EntityOffset, error) {
	var offsets []*egopb.EntityOffset
	for rows.Next() {
		offset := new(egopb.EntityOffset)
		if err := rows.Scan(&offset.ShardNumber, &offset.PersistenceId, &offset.SequenceNumber, &offset.Timestamp, &offset.ConsumedAt); err != nil {
			return nil, err
		}
		offsets = append(offsets, offset)
	}
	return offsets, rows.Err()
}
