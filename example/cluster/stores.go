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
	"github.com/tochemey/ego/v5/offsetstore"
	"github.com/tochemey/ego/v5/persistence"
)

// PostgresEventStore implements persistence.EventsStore using PostgreSQL.
type PostgresEventStore struct {
	pool *pgxpool.Pool
	dsn  string
}

var _ persistence.EventsStore = (*PostgresEventStore)(nil)

// NewPostgresEventStore creates a new PostgreSQL-backed event store.
func NewPostgresEventStore(dsn string) *PostgresEventStore {
	return &PostgresEventStore{dsn: dsn}
}

func (s *PostgresEventStore) Connect(ctx context.Context) error {
	cfg, err := pgxpool.ParseConfig(s.dsn)
	if err != nil {
		return fmt.Errorf("event store config: %w", err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("event store connect: %w", err)
	}
	s.pool = pool
	return nil
}

func (s *PostgresEventStore) Disconnect(_ context.Context) error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

func (s *PostgresEventStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *PostgresEventStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, event := range events {
		payload, err := proto.Marshal(event.GetEvent())
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		manifest := string(event.GetEvent().GetTypeUrl())

		// a plain insert: the primary key rejects an event that is already
		// stored, which fails the whole write
		_, err = tx.Exec(ctx, `
			INSERT INTO events_store
				(persistence_id, sequence_number, is_deleted, event_payload, event_manifest,
				 timestamp, shard_number, encryption_key_id, is_encrypted)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			event.GetPersistenceId(),
			event.GetSequenceNumber(),
			event.GetIsDeleted(),
			payload,
			manifest,
			event.GetTimestamp(),
			event.GetShard(),
			event.GetEncryptionKeyId(),
			event.GetIsEncrypted(),
		)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (s *PostgresEventStore) DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM events_store WHERE persistence_id=$1 AND sequence_number<=$2`,
		persistenceID, toSequenceNumber)
	return err
}

func (s *PostgresEventStore) ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber, limit uint64) ([]*egopb.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT persistence_id, sequence_number, is_deleted, event_payload, event_manifest,
		       timestamp, shard_number, encryption_key_id, is_encrypted
		FROM events_store
		WHERE persistence_id=$1 AND sequence_number>=$2 AND sequence_number<=$3
		ORDER BY sequence_number ASC
		LIMIT $4`,
		persistenceID, fromSequenceNumber, toSequenceNumber, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *PostgresEventStore) GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error) {
	events, err := s.pool.Query(ctx, `
		SELECT persistence_id, sequence_number, is_deleted, event_payload, event_manifest,
		       timestamp, shard_number, encryption_key_id, is_encrypted
		FROM events_store
		WHERE persistence_id=$1
		ORDER BY sequence_number DESC
		LIMIT 1`, persistenceID)
	if err != nil {
		return nil, err
	}
	defer events.Close()

	result, err := scanEvents(events)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result[0], nil
}

func (s *PostgresEventStore) PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) ([]string, string, error) {
	var rows pgx.Rows
	var err error
	if pageToken == "" {
		rows, err = s.pool.Query(ctx,
			`SELECT DISTINCT persistence_id FROM events_store ORDER BY persistence_id LIMIT $1`,
			pageSize)
	} else {
		rows, err = s.pool.Query(ctx,
			`SELECT DISTINCT persistence_id FROM events_store WHERE persistence_id > $1 ORDER BY persistence_id LIMIT $2`,
			pageToken, pageSize)
	}
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, "", err
		}
		ids = append(ids, id)
	}

	var next string
	if len(ids) == int(pageSize) {
		next = ids[len(ids)-1]
	}
	return ids, next, rows.Err()
}

func (s *PostgresEventStore) EventsByShard(ctx context.Context, shardNumber uint64, fromTimestamp int64, limit uint64) ([]*egopb.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT persistence_id, sequence_number, is_deleted, event_payload, event_manifest,
		       timestamp, shard_number, encryption_key_id, is_encrypted
		FROM events_store
		WHERE shard_number=$1 AND timestamp >= $2
		ORDER BY timestamp ASC, sequence_number ASC, persistence_id ASC
		LIMIT $3`,
		shardNumber, fromTimestamp, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanEvents(rows)
}

func (s *PostgresEventStore) ShardOffsets(ctx context.Context) (map[uint64]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT shard_number, MAX(timestamp) FROM events_store GROUP BY shard_number`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	offsets := make(map[uint64]int64)
	for rows.Next() {
		var (
			shard  uint64
			offset int64
		)
		if err := rows.Scan(&shard, &offset); err != nil {
			return nil, err
		}
		offsets[shard] = offset
	}
	return offsets, rows.Err()
}

// scanEvents reads rows into egopb.Event slices.
func scanEvents(rows pgx.Rows) ([]*egopb.Event, error) {
	var events []*egopb.Event
	for rows.Next() {
		var (
			persistenceID   string
			sequenceNumber  uint64
			isDeleted       bool
			payload         []byte
			manifest        string
			timestamp       int64
			shardNumber     uint64
			encryptionKeyID string
			isEncrypted     bool
		)
		if err := rows.Scan(&persistenceID, &sequenceNumber, &isDeleted, &payload, &manifest,
			&timestamp, &shardNumber, &encryptionKeyID, &isEncrypted); err != nil {
			return nil, err
		}

		eventAny := &anypb.Any{TypeUrl: manifest}
		if err := proto.Unmarshal(payload, eventAny); err != nil {
			return nil, fmt.Errorf("unmarshal event payload: %w", err)
		}

		events = append(events, &egopb.Event{
			PersistenceId:   persistenceID,
			SequenceNumber:  sequenceNumber,
			IsDeleted:       isDeleted,
			Event:           eventAny,
			Timestamp:       timestamp,
			Shard:           shardNumber,
			EncryptionKeyId: encryptionKeyID,
			IsEncrypted:     isEncrypted,
		})
	}
	return events, rows.Err()
}

// PostgresOffsetStore implements offsetstore.OffsetStore using PostgreSQL.
type PostgresOffsetStore struct {
	pool *pgxpool.Pool
	dsn  string
}

var _ offsetstore.OffsetStore = (*PostgresOffsetStore)(nil)

// NewPostgresOffsetStore creates a new PostgreSQL-backed offset store.
func NewPostgresOffsetStore(dsn string) *PostgresOffsetStore {
	return &PostgresOffsetStore{dsn: dsn}
}

func (s *PostgresOffsetStore) Connect(ctx context.Context) error {
	cfg, err := pgxpool.ParseConfig(s.dsn)
	if err != nil {
		return fmt.Errorf("offset store config: %w", err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("offset store connect: %w", err)
	}
	s.pool = pool
	return nil
}

func (s *PostgresOffsetStore) Disconnect(_ context.Context) error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

func (s *PostgresOffsetStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// WriteOffsets inserts one row per offset in a single transaction, sent as one
// batch so a buffer of offsets costs one round trip. Writing the same offset
// twice is harmless: the row is identical.
func (s *PostgresOffsetStore) WriteOffsets(ctx context.Context, projectionName string, offsets []*egopb.EntityOffset) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, offset := range offsets {
		batch.Queue(`
			INSERT INTO offsets_store
				(projection_name, shard_number, timestamp, persistence_id, sequence_number, consumed_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT DO NOTHING`,
			projectionName,
			offset.GetShardNumber(),
			offset.GetTimestamp(),
			offset.GetPersistenceId(),
			offset.GetSequenceNumber(),
			offset.GetConsumedAt(),
		)
	}

	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("insert offsets: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *PostgresOffsetStore) ReadOffsets(ctx context.Context, projectionName string, shardNumber uint64, limit uint64) ([]*egopb.EntityOffset, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT shard_number, persistence_id, sequence_number, timestamp, consumed_at
		FROM offsets_store
		WHERE projection_name=$1 AND shard_number=$2
		ORDER BY timestamp DESC
		LIMIT $3`,
		projectionName, shardNumber, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanOffsets(rows)
}

func (s *PostgresOffsetStore) ReadEntityOffset(ctx context.Context, projectionName string, shardNumber uint64, persistenceID string) (*egopb.EntityOffset, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT shard_number, persistence_id, sequence_number, timestamp, consumed_at
		FROM offsets_store
		WHERE projection_name=$1 AND shard_number=$2 AND persistence_id=$3
		ORDER BY sequence_number DESC
		LIMIT 1`,
		projectionName, shardNumber, persistenceID)
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

func (s *PostgresOffsetStore) LatestOffsets(ctx context.Context, projectionName string) (map[uint64]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT shard_number, MAX(timestamp) FROM offsets_store
		WHERE projection_name=$1 GROUP BY shard_number`,
		projectionName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	latest := make(map[uint64]int64)
	for rows.Next() {
		var (
			shard     uint64
			timestamp int64
		)
		if err := rows.Scan(&shard, &timestamp); err != nil {
			return nil, err
		}
		latest[shard] = timestamp
	}
	return latest, rows.Err()
}

func (s *PostgresOffsetStore) DeleteOffsetsBefore(ctx context.Context, projectionName string, shardNumber uint64, before, consumedBefore int64) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM offsets_store
		WHERE projection_name=$1 AND shard_number=$2 AND timestamp < $3 AND consumed_at < $4`,
		projectionName, shardNumber, before, consumedBefore)
	return err
}

// ResetOffset deletes the offsets of the projection and records its start
// offset, in a single transaction.
func (s *PostgresOffsetStore) ResetOffset(ctx context.Context, projectionName string, value int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM offsets_store WHERE projection_name=$1`, projectionName); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO offsets_start (projection_name, start_offset) VALUES ($1, $2)
		ON CONFLICT (projection_name) DO UPDATE SET start_offset = EXCLUDED.start_offset`,
		projectionName, value); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresOffsetStore) StartOffset(ctx context.Context, projectionName string) (int64, error) {
	var start int64
	err := s.pool.QueryRow(ctx, `SELECT start_offset FROM offsets_start WHERE projection_name=$1`, projectionName).Scan(&start)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return start, err
}

// DeleteOffset removes the offsets and the start offset of the given
// projection. Deleting the offsets of a projection that has none is not an
// error.
func (s *PostgresOffsetStore) DeleteOffset(ctx context.Context, projectionName string) error {
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

// scanOffsets reads rows into egopb.EntityOffset slices.
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
