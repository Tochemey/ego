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
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// snapshotColumns are the columns a snapshot is read from, in scanSnapshot order.
	snapshotColumns = "persistence_id, sequence_number, state_payload, state_manifest, timestamp, encryption_key_id, is_encrypted"

	// writeSnapshotSQL stores a snapshot, replacing one stored at the same sequence number.
	writeSnapshotSQL = "INSERT INTO snapshots_store (" + snapshotColumns + ") VALUES ($1, $2, $3, $4, $5, $6, $7) " +
		"ON CONFLICT (persistence_id, sequence_number) DO UPDATE SET " +
		"state_payload = EXCLUDED.state_payload, state_manifest = EXCLUDED.state_manifest, " +
		"timestamp = EXCLUDED.timestamp, encryption_key_id = EXCLUDED.encryption_key_id, " +
		"is_encrypted = EXCLUDED.is_encrypted"

	latestSnapshotSQL = "SELECT " + snapshotColumns + " FROM snapshots_store " +
		"WHERE persistence_id = $1 ORDER BY sequence_number DESC LIMIT 1"

	deleteSnapshotsSQL = "DELETE FROM snapshots_store WHERE persistence_id = $1 AND sequence_number <= $2"
)

// SnapshotStore implements persistence.SnapshotStore on the snapshots_store
// table of resources/snapshotstore_postgres.sql.
//
// A snapshot's state is stored as the Value of its Any and its manifest as
// the TypeUrl, so a read returns the Any that was written.
type SnapshotStore struct {
	// pool runs the statements. The application owns it.
	pool Pool
}

// enforce the interface implementation
var _ persistence.SnapshotStore = (*SnapshotStore)(nil)

// NewSnapshotStore creates a snapshot store running on the given pool. The
// store never closes the pool, so the pool can be shared with the other
// stores and with the application.
func NewSnapshotStore(pool Pool) *SnapshotStore {
	return &SnapshotStore{pool: pool}
}

// Connect verifies the database is reachable.
func (s *SnapshotStore) Connect(ctx context.Context) error {
	return connect(ctx, s.pool)
}

// Disconnect releases nothing: the pool belongs to the application.
func (s *SnapshotStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the database is reachable.
func (s *SnapshotStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// WriteSnapshot stores the snapshot, replacing a snapshot of the persistence
// ID stored at the same sequence number.
func (s *SnapshotStore) WriteSnapshot(ctx context.Context, snapshot *egopb.Snapshot) error {
	if _, err := s.pool.Exec(ctx, writeSnapshotSQL,
		snapshot.GetPersistenceId(),
		int64(snapshot.GetSequenceNumber()),
		payloadOf(snapshot.GetState()),
		snapshot.GetState().GetTypeUrl(),
		snapshot.GetTimestamp(),
		snapshot.GetEncryptionKeyId(),
		snapshot.GetIsEncrypted(),
	); err != nil {
		return fmt.Errorf("postgres: failed to write the snapshot: %w", err)
	}

	return nil
}

// GetLatestSnapshot returns the snapshot of the persistence ID with the highest
// sequence number, or nil when it has none.
func (s *SnapshotStore) GetLatestSnapshot(ctx context.Context, persistenceID string) (*egopb.Snapshot, error) {
	snapshot := &egopb.Snapshot{State: new(anypb.Any)}
	err := s.pool.QueryRow(ctx, latestSnapshotSQL, persistenceID).Scan(
		&snapshot.PersistenceId,
		&snapshot.SequenceNumber,
		&snapshot.State.Value,
		&snapshot.State.TypeUrl,
		&snapshot.Timestamp,
		&snapshot.EncryptionKeyId,
		&snapshot.IsEncrypted,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the latest snapshot: %w", err)
	}

	return snapshot, nil
}

// DeleteSnapshots deletes the snapshots of the persistence ID up to the given
// sequence number, inclusive.
func (s *SnapshotStore) DeleteSnapshots(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	if _, err := s.pool.Exec(ctx, deleteSnapshotsSQL, persistenceID, int64(toSequenceNumber)); err != nil {
		return fmt.Errorf("postgres: failed to delete the snapshots: %w", err)
	}

	return nil
}
