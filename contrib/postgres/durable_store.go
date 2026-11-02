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
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// writeStateSQL stores the state of a persistence ID, replacing the one stored.
	writeStateSQL = "INSERT INTO states_store " +
		"(persistence_id, version_number, state_payload, state_manifest, timestamp, shard_number) " +
		"VALUES ($1, $2, $3, $4, $5, $6) " +
		"ON CONFLICT (persistence_id) DO UPDATE SET " +
		"version_number = EXCLUDED.version_number, state_payload = EXCLUDED.state_payload, " +
		"state_manifest = EXCLUDED.state_manifest, timestamp = EXCLUDED.timestamp, " +
		"shard_number = EXCLUDED.shard_number"

	latestStateSQL = "SELECT version_number, state_payload, state_manifest, timestamp, shard_number " +
		"FROM states_store WHERE persistence_id = $1"

	deleteStateSQL = "DELETE FROM states_store WHERE persistence_id = $1"
)

// DurableStore implements persistence.StateStore on the states_store table of
// resources/durablestore_postgres.sql.
//
// A state is stored as the Value of its Any and its manifest as the TypeUrl,
// so a read returns the Any that was written. A tombstone left by DeleteState
// has an empty manifest and reads back with no resulting state.
type DurableStore struct {
	// pool runs the statements. The application owns it.
	pool Pool
}

// enforce the interface implementation
var _ persistence.StateStore = (*DurableStore)(nil)

// NewDurableStore creates a durable state store running on the given pool.
// The store never closes the pool, so the pool can be shared with the other
// stores and with the application.
func NewDurableStore(pool Pool) *DurableStore {
	return &DurableStore{pool: pool}
}

// Connect verifies the database is reachable.
func (s *DurableStore) Connect(ctx context.Context) error {
	return connect(ctx, s.pool)
}

// Disconnect releases nothing: the pool belongs to the application.
func (s *DurableStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the database is reachable.
func (s *DurableStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// WriteState stores the state of its persistence ID, replacing the one stored.
func (s *DurableStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	if _, err := s.pool.Exec(ctx, writeStateSQL,
		state.GetPersistenceId(),
		int64(state.GetVersionNumber()),
		payloadOf(state.GetResultingState()),
		state.GetResultingState().GetTypeUrl(),
		state.GetTimestamp(),
		int64(state.GetShard()),
	); err != nil {
		return fmt.Errorf("postgres: failed to write the durable state: %w", err)
	}

	return nil
}

// GetLatestState returns the state of the persistence ID, or nil when it has
// none. A tombstone returns its version with no resulting state.
func (s *DurableStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	var (
		state    = &egopb.DurableState{PersistenceId: persistenceID}
		payload  []byte
		manifest string
	)

	err := s.pool.QueryRow(ctx, latestStateSQL, persistenceID).Scan(
		&state.VersionNumber,
		&payload,
		&manifest,
		&state.Timestamp,
		&state.Shard,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the durable state: %w", err)
	}

	if manifest != "" {
		state.ResultingState = &anypb.Any{TypeUrl: manifest, Value: payload}
	}

	return state, nil
}

// DeleteState deletes the state of the persistence ID. With version zero the
// row is removed; with a positive version a tombstone of that version
// replaces it, so the versions of the persistence ID keep increasing across
// the deletion. A persistence ID with no state is not an error.
func (s *DurableStore) DeleteState(ctx context.Context, persistenceID string, version uint64) error {
	var err error
	if version == 0 {
		_, err = s.pool.Exec(ctx, deleteStateSQL, persistenceID)
	} else {
		_, err = s.pool.Exec(ctx, writeStateSQL,
			persistenceID,
			int64(version),
			[]byte{},
			"",
			time.Now().UnixNano(),
			int64(persistence.ShardFor(persistenceID)),
		)
	}

	if err != nil {
		return fmt.Errorf("postgres: failed to delete the durable state: %w", err)
	}

	return nil
}
