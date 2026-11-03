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
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// writeStateStatement replaces the row of a persistence ID. A Cassandra
	// INSERT is an upsert, so writing the same row again leaves it unchanged.
	writeStateStatement = `INSERT INTO states_store (persistence_id, version_number, state_payload, state_manifest, timestamp, shard_number) VALUES (?, ?, ?, ?, ?, ?)`
	// latestStateStatement reads the row of a persistence ID by its partition key.
	latestStateStatement = `SELECT version_number, state_payload, state_manifest, timestamp, shard_number FROM states_store WHERE persistence_id = ?`
	// deleteStateStatement removes the row of a persistence ID.
	deleteStateStatement = `DELETE FROM states_store WHERE persistence_id = ?`
)

// DurableStore persists the latest durable state of every persistence ID in
// the states_store table, one row per persistence ID.
//
// It runs every statement on the session it was created with and with the
// session's consistency. Read and write with QUORUM or LOCAL_QUORUM, so that an
// entity recovering on another node reads the latest state it wrote. Every
// statement is marked idempotent: each one replaces or removes a whole row, so
// the driver may retry it safely.
//
// A DurableStore is safe for concurrent use.
type DurableStore struct {
	// session runs the store's statements. The application owns it.
	session *gocql.Session
}

// enforce interface implementation
var _ persistence.StateStore = (*DurableStore)(nil)

// NewDurableStore creates a durable state store on the given session.
//
// The session stays owned by the caller: the store never closes it, so the same
// session can serve the application's own queries. Close it after the engine
// has stopped.
func NewDurableStore(session *gocql.Session) *DurableStore {
	return &DurableStore{session: session}
}

// Connect verifies the session reaches the cluster. The session is connected
// already when the application creates it.
func (s *DurableStore) Connect(ctx context.Context) error {
	return s.Ping(ctx)
}

// Disconnect releases nothing: the session belongs to the application, which
// closes it.
func (s *DurableStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the cluster answers a query on the session.
func (s *DurableStore) Ping(ctx context.Context) error {
	return ping(ctx, s.session)
}

// WriteState persists the durable state of a persistence ID, replacing the
// state stored before.
func (s *DurableStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	resultingState := state.GetResultingState()
	return s.writeRow(ctx,
		state.GetPersistenceId(),
		state.GetVersionNumber(),
		resultingState.GetValue(),
		resultingState.GetTypeUrl(),
		state.GetTimestamp(),
		state.GetShard())
}

// GetLatestState returns the durable state of the persistence ID, or nil when
// none is stored. A tombstone left by DeleteState comes back with its version
// and no resulting state.
func (s *DurableStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	var (
		versionNumber uint64
		payload       []byte
		manifest      string
		timestamp     int64
		shardNumber   uint64
	)

	err := s.session.Query(latestStateStatement, persistenceID).
		Idempotent(true).
		ScanContext(ctx, &versionNumber, &payload, &manifest, &timestamp, &shardNumber)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("cassandra: failed to read the durable state: %w", err)
	}

	state := &egopb.DurableState{
		PersistenceId: persistenceID,
		VersionNumber: versionNumber,
		Timestamp:     timestamp,
		Shard:         shardNumber,
	}

	if manifest != "" {
		state.ResultingState = &anypb.Any{TypeUrl: manifest, Value: payload}
	}

	return state, nil
}

// DeleteState deletes the durable state of the persistence ID.
//
// With a version greater than zero the row becomes a tombstone that keeps the
// version, so the versions of the persistence ID keep increasing after the
// deletion. With version zero the row is removed. A persistence ID with no
// state is not an error.
func (s *DurableStore) DeleteState(ctx context.Context, persistenceID string, version uint64) error {
	if version > 0 {
		return s.writeRow(ctx, persistenceID, version, nil, "", time.Now().UnixNano(), persistence.ShardFor(persistenceID))
	}

	if err := s.session.Query(deleteStateStatement, persistenceID).Idempotent(true).ExecContext(ctx); err != nil {
		return fmt.Errorf("cassandra: failed to delete the durable state: %w", err)
	}

	return nil
}

// writeRow replaces the row of the persistence ID with the given columns.
func (s *DurableStore) writeRow(ctx context.Context, persistenceID string, versionNumber uint64, payload []byte, manifest string, timestamp int64, shardNumber uint64) error {
	err := s.session.Query(writeStateStatement, persistenceID, versionNumber, payload, manifest, timestamp, shardNumber).
		Idempotent(true).
		ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("cassandra: failed to write the durable state: %w", err)
	}

	return nil
}
