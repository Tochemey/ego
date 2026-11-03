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

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// snapshotColumns are the columns of snapshots_store, in the order the statements bind and scan them.
	snapshotColumns = "persistence_id, sequence_number, state_payload, state_manifest, timestamp, " +
		"encryption_key_id, is_encrypted"

	// writeSnapshotQuery stores a snapshot, replacing the one of the same sequence number.
	writeSnapshotQuery = "INSERT INTO snapshots_store (" + snapshotColumns + ") VALUES (?, ?, ?, ?, ?, ?, ?) AS written " +
		"ON DUPLICATE KEY UPDATE " +
		"state_payload = written.state_payload, state_manifest = written.state_manifest, " +
		"timestamp = written.timestamp, encryption_key_id = written.encryption_key_id, " +
		"is_encrypted = written.is_encrypted"
	// latestSnapshotQuery reads an entity's newest snapshot through the primary key.
	latestSnapshotQuery = "SELECT " + snapshotColumns + " FROM snapshots_store " +
		"WHERE persistence_id = ? ORDER BY sequence_number DESC LIMIT 1"
	// deleteSnapshotsQuery deletes an entity's snapshots up to a sequence number through the primary key.
	deleteSnapshotsQuery = "DELETE FROM snapshots_store WHERE persistence_id = ? AND sequence_number <= ?"
)

// SnapshotStore implements persistence.SnapshotStore on the snapshots_store table of
// resources/snapshotstore_mysql.sql.
type SnapshotStore struct {
	// db is the shared database handle, owned by the caller.
	db *sql.DB
}

// enforce interface implementation
var _ persistence.SnapshotStore = (*SnapshotStore)(nil)

// NewSnapshotStore creates a snapshot store running on db. The store never closes db, so
// the handle can be shared with the other stores and with the application.
func NewSnapshotStore(db *sql.DB) *SnapshotStore {
	return &SnapshotStore{db: db}
}

// Connect verifies the database is reachable.
func (s *SnapshotStore) Connect(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Disconnect releases nothing: the database handle stays open for its owner to close.
func (s *SnapshotStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the database is reachable.
func (s *SnapshotStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// WriteSnapshot stores the snapshot, replacing a snapshot of the same persistence ID and sequence number.
func (s *SnapshotStore) WriteSnapshot(ctx context.Context, snapshot *egopb.Snapshot) error {
	_, err := s.db.ExecContext(ctx, writeSnapshotQuery,
		snapshot.GetPersistenceId(),
		snapshot.GetSequenceNumber(),
		blob(snapshot.GetState().GetValue()),
		snapshot.GetState().GetTypeUrl(),
		snapshot.GetTimestamp(),
		snapshot.GetEncryptionKeyId(),
		snapshot.GetIsEncrypted(),
	)
	if err != nil {
		return fmt.Errorf("mysql: failed to write the snapshot: %w", err)
	}

	return nil
}

// GetLatestSnapshot returns the snapshot of the persistence ID with the highest sequence number,
// or nil when the persistence ID has no snapshot.
func (s *SnapshotStore) GetLatestSnapshot(ctx context.Context, persistenceID string) (*egopb.Snapshot, error) {
	snapshot := &egopb.Snapshot{State: new(anypb.Any)}
	err := s.db.QueryRowContext(ctx, latestSnapshotQuery, persistenceID).Scan(
		&snapshot.PersistenceId,
		&snapshot.SequenceNumber,
		&snapshot.State.Value,
		&snapshot.State.TypeUrl,
		&snapshot.Timestamp,
		&snapshot.EncryptionKeyId,
		&snapshot.IsEncrypted,
	)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("mysql: failed to read the latest snapshot: %w", err)
	default:
		return snapshot, nil
	}
}

// DeleteSnapshots deletes the snapshots of the persistence ID up to toSequenceNumber, inclusive.
func (s *SnapshotStore) DeleteSnapshots(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	if _, err := s.db.ExecContext(ctx, deleteSnapshotsQuery, persistenceID, toSequenceNumber); err != nil {
		return fmt.Errorf("mysql: failed to delete the snapshots: %w", err)
	}

	return nil
}
