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

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// eventColumns are the columns of events_store, in the order the statements bind and scan them.
	eventColumns = "persistence_id, sequence_number, is_deleted, event_payload, event_manifest, " +
		"timestamp, shard_number, encryption_key_id, is_encrypted"
	// eventColumnCount is the number of columns in eventColumns.
	eventColumnCount = 9

	// selectEvents reads every column of events_store.
	selectEvents = "SELECT " + eventColumns + " FROM events_store "
	// replayEventsQuery reads a range of an entity's events through the primary key.
	replayEventsQuery = selectEvents +
		"WHERE persistence_id = ? AND sequence_number >= ? AND sequence_number <= ? ORDER BY sequence_number LIMIT ?"
	// latestEventQuery reads an entity's newest event through the primary key.
	latestEventQuery = selectEvents + "WHERE persistence_id = ? ORDER BY sequence_number DESC LIMIT 1"
	// eventsByShardQuery reads a shard from a timestamp on through idx_events_store_shard_timestamp,
	// whose column order is the order of the result.
	eventsByShardQuery = selectEvents +
		"WHERE shard_number = ? AND timestamp >= ? ORDER BY timestamp, sequence_number, persistence_id LIMIT ?"
	// deleteEventsQuery deletes an entity's events up to a sequence number through the primary key.
	deleteEventsQuery = "DELETE FROM events_store WHERE persistence_id = ? AND sequence_number <= ?"
	// persistenceIDsQuery reads a page of distinct persistence IDs, in order, through the primary key.
	persistenceIDsQuery = "SELECT DISTINCT persistence_id FROM events_store " +
		"WHERE persistence_id > ? ORDER BY persistence_id LIMIT ?"
	// shardOffsetsQuery reads the newest timestamp of each shard: the recursive CTE lists the
	// shard numbers, and each correlated MAX is a single seek in idx_events_store_shard_timestamp,
	// so its cost depends on the number of shards rather than on the size of the journal. The
	// latest CTE is materialized so that each MAX runs once, not again for the IS NOT NULL filter.
	shardOffsetsQuery = "WITH RECURSIVE shards(shard_number) AS " +
		"(SELECT 0 UNION ALL SELECT shard_number + 1 FROM shards WHERE shard_number < ?), " +
		"latest(shard_number, timestamp) AS MATERIALIZED (SELECT shard_number, " +
		"(SELECT MAX(timestamp) FROM events_store WHERE events_store.shard_number = shards.shard_number) " +
		"FROM shards) " +
		"SELECT shard_number, timestamp FROM latest WHERE timestamp IS NOT NULL"
)

// insertEvents writes events with a plain insert: an event whose persistence ID and
// sequence number are stored already violates the primary key and fails the write.
var insertEvents = insertStatement{
	prefix:  "INSERT INTO events_store (" + eventColumns + ") VALUES ",
	columns: eventColumnCount,
}

// EventsStore implements persistence.EventsStore on the events_store table of a SQLite database.
type EventsStore struct {
	// db is the shared database handle, owned by the caller.
	db *sql.DB
}

// enforce interface implementation
var _ persistence.EventsStore = (*EventsStore)(nil)

// NewEventsStore creates an events store running on db, a handle opened with NewDB.
// The store never closes db.
func NewEventsStore(db *sql.DB) *EventsStore {
	return &EventsStore{db: db}
}

// Connect verifies the database is reachable.
func (s *EventsStore) Connect(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Disconnect releases nothing: the database handle stays open for its owner to close.
func (s *EventsStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the database is reachable.
func (s *EventsStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// WriteEvents stores the events atomically: either every event is stored or none is.
// An event that is stored already fails the whole write.
func (s *EventsStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	args := make([]any, 0, len(events)*eventColumnCount)
	for _, event := range events {
		args = append(args,
			event.GetPersistenceId(),
			event.GetSequenceNumber(),
			event.GetIsDeleted(),
			blob(event.GetEvent().GetValue()),
			event.GetEvent().GetTypeUrl(),
			event.GetTimestamp(),
			event.GetShard(),
			event.GetEncryptionKeyId(),
			event.GetIsEncrypted(),
		)
	}

	if err := insertEvents.write(ctx, s.db, args); err != nil {
		return fmt.Errorf("sqlite: failed to write the events: %w", err)
	}

	return nil
}

// DeleteEvents deletes the events of the persistence ID up to toSequenceNumber, inclusive.
func (s *EventsStore) DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	if _, err := s.db.ExecContext(ctx, deleteEventsQuery, persistenceID, toSequenceNumber); err != nil {
		return fmt.Errorf("sqlite: failed to delete the events: %w", err)
	}

	return nil
}

// ReplayEvents returns at most limit events of the persistence ID whose sequence numbers lie
// between fromSequenceNumber and toSequenceNumber, both inclusive, in sequence order.
func (s *EventsStore) ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, limit uint64) ([]*egopb.Event, error) {
	rows, err := s.db.QueryContext(ctx, replayEventsQuery, persistenceID, fromSequenceNumber, toSequenceNumber, limit)
	events, err := collect(rows, err, limit, scanEvent)
	if err != nil {
		return nil, fmt.Errorf("sqlite: failed to replay the events: %w", err)
	}

	return events, nil
}

// GetLatestEvent returns the event of the persistence ID with the highest sequence number,
// or nil when the persistence ID has no event.
func (s *EventsStore) GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error) {
	event, err := scanEvent(s.db.QueryRowContext(ctx, latestEventQuery, persistenceID))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("sqlite: failed to read the latest event: %w", err)
	default:
		return event, nil
	}
}

// PersistenceIDs returns a page of at most pageSize distinct persistence IDs, in order, after
// pageToken; an empty pageToken starts from the first one. The next page token is the last
// persistence ID of a full page, and empty once there are no more persistence IDs.
func (s *EventsStore) PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error) {
	rows, err := s.db.QueryContext(ctx, persistenceIDsQuery, pageToken, pageSize)
	persistenceIDs, err = collect(rows, err, pageSize, scanPersistenceID)
	if err != nil {
		return nil, "", fmt.Errorf("sqlite: failed to list the persistence IDs: %w", err)
	}

	if len(persistenceIDs) > 0 && uint64(len(persistenceIDs)) == pageSize {
		nextPageToken = persistenceIDs[len(persistenceIDs)-1]
	}

	return persistenceIDs, nextPageToken, nil
}

// EventsByShard returns at most limit events of the shard whose timestamp is at or after
// fromTimestamp, ordered by timestamp, then sequence number, then persistence ID.
func (s *EventsStore) EventsByShard(ctx context.Context, shardNumber uint64, fromTimestamp int64, limit uint64) ([]*egopb.Event, error) {
	rows, err := s.db.QueryContext(ctx, eventsByShardQuery, shardNumber, fromTimestamp, limit)
	events, err := collect(rows, err, limit, scanEvent)
	if err != nil {
		return nil, fmt.Errorf("sqlite: failed to read the shard events: %w", err)
	}

	return events, nil
}

// ShardOffsets returns every shard that holds events mapped to the timestamp of its newest event.
// An empty journal yields an empty map.
func (s *EventsStore) ShardOffsets(ctx context.Context) (map[uint64]int64, error) {
	rows, err := s.db.QueryContext(ctx, shardOffsetsQuery, persistence.NumberOfShards-1)
	offsets, err := collectShardTimestamps(rows, err)
	if err != nil {
		return nil, fmt.Errorf("sqlite: failed to read the shard offsets: %w", err)
	}

	return offsets, nil
}

// scanEvent reads an event from a row of eventColumns.
func scanEvent(row scanner) (*egopb.Event, error) {
	event := &egopb.Event{Event: new(anypb.Any)}
	err := row.Scan(
		&event.PersistenceId,
		&event.SequenceNumber,
		&event.IsDeleted,
		&event.Event.Value,
		&event.Event.TypeUrl,
		&event.Timestamp,
		&event.Shard,
		&event.EncryptionKeyId,
		&event.IsEncrypted,
	)

	return event, err
}

// scanPersistenceID reads a persistence ID from a row.
func scanPersistenceID(row scanner) (string, error) {
	var persistenceID string
	err := row.Scan(&persistenceID)
	return persistenceID, err
}
