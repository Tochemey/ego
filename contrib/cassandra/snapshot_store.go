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
	"context"
	"errors"
	"fmt"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// writeSnapshotStatement stores a snapshot. A Cassandra INSERT is an upsert,
	// so writing the same snapshot again leaves its row unchanged.
	writeSnapshotStatement = `INSERT INTO snapshots_store (persistence_id, sequence_number, state_payload, state_manifest, timestamp, encryption_key_id, is_encrypted) VALUES (?, ?, ?, ?, ?, ?, ?)`
	// latestSnapshotStatement reads the first row of a persistence ID's
	// partition, which holds the highest sequence number.
	latestSnapshotStatement = `SELECT sequence_number, state_payload, state_manifest, timestamp, encryption_key_id, is_encrypted FROM snapshots_store WHERE persistence_id = ? LIMIT 1`
	// deleteSnapshotsStatement removes the snapshots of a persistence ID up to a
	// sequence number, inclusive, with one range deletion.
	deleteSnapshotsStatement = `DELETE FROM snapshots_store WHERE persistence_id = ? AND sequence_number <= ?`
)

// SnapshotStore persists the snapshots of every persistence ID in the
// snapshots_store table of resources/snapshotstore_cassandra.cql, one
// partition per persistence ID.
//
// It runs every statement on the session it was created with and with the
// session's consistency. Read and write with QUORUM or LOCAL_QUORUM, so that an
// entity recovering on another node reads the latest snapshot it wrote. Every
// statement is marked idempotent, so the driver may retry it safely.
//
// A SnapshotStore is safe for concurrent use.
type SnapshotStore struct {
	// session runs the store's statements. The application owns it.
	session *gocql.Session
}

// enforce interface implementation
var _ persistence.SnapshotStore = (*SnapshotStore)(nil)

// NewSnapshotStore creates a snapshot store on the given session.
//
// The session stays owned by the caller: the store never closes it, so the same
// session can serve the other stores and the application's own queries.
func NewSnapshotStore(session *gocql.Session) *SnapshotStore {
	return &SnapshotStore{session: session}
}

// Connect verifies the session reaches the cluster. The session is connected
// already when the application creates it.
func (s *SnapshotStore) Connect(ctx context.Context) error {
	return ping(ctx, s.session)
}

// Disconnect releases nothing: the session belongs to the application, which
// closes it.
func (s *SnapshotStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the cluster answers a query on the session.
func (s *SnapshotStore) Ping(ctx context.Context) error {
	return ping(ctx, s.session)
}

// WriteSnapshot persists a snapshot of a persistence ID.
func (s *SnapshotStore) WriteSnapshot(ctx context.Context, snapshot *egopb.Snapshot) error {
	state := snapshot.GetState()
	err := s.session.Query(writeSnapshotStatement,
		snapshot.GetPersistenceId(),
		snapshot.GetSequenceNumber(),
		state.GetValue(),
		state.GetTypeUrl(),
		snapshot.GetTimestamp(),
		snapshot.GetEncryptionKeyId(),
		snapshot.GetIsEncrypted(),
	).Idempotent(true).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("cassandra: failed to write the snapshot: %w", err)
	}

	return nil
}

// GetLatestSnapshot returns the snapshot of the persistence ID with the highest
// sequence number, or nil when it has none.
func (s *SnapshotStore) GetLatestSnapshot(ctx context.Context, persistenceID string) (*egopb.Snapshot, error) {
	var (
		snapshot = &egopb.Snapshot{PersistenceId: persistenceID}
		payload  []byte
		manifest string
	)

	err := s.session.Query(latestSnapshotStatement, persistenceID).
		Idempotent(true).
		ScanContext(ctx, &snapshot.SequenceNumber, &payload, &manifest, &snapshot.Timestamp, &snapshot.EncryptionKeyId, &snapshot.IsEncrypted)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("cassandra: failed to read the snapshot: %w", err)
	}

	if manifest != "" {
		snapshot.State = &anypb.Any{TypeUrl: manifest, Value: payload}
	}

	return snapshot, nil
}

// DeleteSnapshots deletes the snapshots of the persistence ID up to the given
// sequence number, inclusive.
func (s *SnapshotStore) DeleteSnapshots(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	if err := s.session.Query(deleteSnapshotsStatement, persistenceID, toSequenceNumber).Idempotent(true).ExecContext(ctx); err != nil {
		return fmt.Errorf("cassandra: failed to delete the snapshots: %w", err)
	}

	return nil
}
