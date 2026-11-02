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
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// stateColumns are the columns of states_store, in the order the statements bind and scan them.
	stateColumns = "persistence_id, version_number, state_payload, state_manifest, timestamp, shard_number"

	// writeStateQuery stores the state of an entity, replacing its previous state.
	writeStateQuery = "INSERT INTO states_store (" + stateColumns + ") VALUES (?, ?, ?, ?, ?, ?) " +
		"ON CONFLICT (persistence_id) DO UPDATE SET " +
		"version_number = excluded.version_number, state_payload = excluded.state_payload, " +
		"state_manifest = excluded.state_manifest, timestamp = excluded.timestamp, " +
		"shard_number = excluded.shard_number"
	// latestStateQuery reads the state of an entity through the primary key.
	latestStateQuery = "SELECT " + stateColumns + " FROM states_store WHERE persistence_id = ?"
	// deleteStateQuery removes the state of an entity through the primary key.
	deleteStateQuery = "DELETE FROM states_store WHERE persistence_id = ?"
	// tombstoneManifest is the manifest of a tombstone, which has no state.
	tombstoneManifest = ""
)

// DurableStore implements persistence.StateStore on the states_store table of a SQLite database.
type DurableStore struct {
	// db is the shared database handle, owned by the caller.
	db *sql.DB
}

// enforce interface implementation
var _ persistence.StateStore = (*DurableStore)(nil)

// NewDurableStore creates a durable state store running on db, a handle opened with NewDB.
// The store never closes db.
func NewDurableStore(db *sql.DB) *DurableStore {
	return &DurableStore{db: db}
}

// Connect verifies the database is reachable.
func (s *DurableStore) Connect(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Disconnect releases nothing: the database handle stays open for its owner to close.
func (s *DurableStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the database is reachable.
func (s *DurableStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// WriteState stores the state of the persistence ID, replacing its previous state.
func (s *DurableStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	_, err := s.db.ExecContext(ctx, writeStateQuery,
		state.GetPersistenceId(),
		state.GetVersionNumber(),
		blob(state.GetResultingState().GetValue()),
		state.GetResultingState().GetTypeUrl(),
		state.GetTimestamp(),
		state.GetShard(),
	)
	if err != nil {
		return fmt.Errorf("sqlite: failed to write the durable state: %w", err)
	}

	return nil
}

// GetLatestState returns the state of the persistence ID, or nil when it has none. The state of
// a tombstone, which DeleteState leaves with a positive version, has no resulting state.
func (s *DurableStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	var (
		state    = new(egopb.DurableState)
		payload  []byte
		manifest string
	)

	err := s.db.QueryRowContext(ctx, latestStateQuery, persistenceID).Scan(
		&state.PersistenceId,
		&state.VersionNumber,
		&payload,
		&manifest,
		&state.Timestamp,
		&state.Shard,
	)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("sqlite: failed to read the durable state: %w", err)
	}

	if manifest != tombstoneManifest {
		state.ResultingState = &anypb.Any{TypeUrl: manifest, Value: payload}
	}

	return state, nil
}

// DeleteState deletes the state of the persistence ID. With version zero the record is removed;
// with a positive version it becomes a tombstone carrying that version and no state, so the
// versions of the persistence ID keep increasing across the deletion. A persistence ID that has
// no state is not an error.
func (s *DurableStore) DeleteState(ctx context.Context, persistenceID string, version uint64) error {
	var err error
	if version == 0 {
		_, err = s.db.ExecContext(ctx, deleteStateQuery, persistenceID)
	} else {
		_, err = s.db.ExecContext(ctx, writeStateQuery,
			persistenceID,
			version,
			blob(nil),
			tombstoneManifest,
			time.Now().UnixNano(),
			persistence.ShardFor(persistenceID),
		)
	}

	if err != nil {
		return fmt.Errorf("sqlite: failed to delete the durable state: %w", err)
	}

	return nil
}
