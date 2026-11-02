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
	// eventColumns are the columns an event is read from, in scanEvent order.
	eventColumns = "persistence_id, sequence_number, is_deleted, event_payload, event_manifest, timestamp, shard_number, encryption_key_id, is_encrypted"

	// insertEventsSQL inserts any number of events in one statement, one array
	// per column. It has no ON CONFLICT clause: the primary key rejects an event
	// already stored, which fails the whole statement.
	insertEventsSQL = "INSERT INTO events_store (" + eventColumns + ") " +
		"SELECT * FROM unnest($1::text[], $2::bigint[], $3::boolean[], $4::bytea[], $5::text[], $6::bigint[], $7::bigint[], $8::text[], $9::boolean[])"

	deleteEventsSQL = "DELETE FROM events_store WHERE persistence_id = $1 AND sequence_number <= $2"

	replayEventsSQL = "SELECT " + eventColumns + " FROM events_store " +
		"WHERE persistence_id = $1 AND sequence_number >= $2 AND sequence_number <= $3 " +
		"ORDER BY sequence_number LIMIT $4"

	latestEventSQL = "SELECT " + eventColumns + " FROM events_store " +
		"WHERE persistence_id = $1 ORDER BY sequence_number DESC LIMIT 1"

	// persistenceIDsSQL reads the page after the given persistence ID; the first
	// page passes an empty one.
	persistenceIDsSQL = "SELECT DISTINCT persistence_id FROM events_store " +
		"WHERE persistence_id > $1 ORDER BY persistence_id LIMIT $2"

	eventsByShardSQL = "SELECT " + eventColumns + " FROM events_store " +
		"WHERE shard_number = $1 AND timestamp >= $2 " +
		"ORDER BY timestamp, sequence_number, persistence_id LIMIT $3"

	// shardOffsetsSQL reads the newest index entry of every shard, so its cost
	// depends on the number of shards, not on the size of the journal.
	shardOffsetsSQL = "SELECT shards.shard_number, latest.timestamp " +
		"FROM generate_series(0, $1::bigint) AS shards(shard_number) " +
		"CROSS JOIN LATERAL (SELECT timestamp FROM events_store " +
		"WHERE shard_number = shards.shard_number ORDER BY timestamp DESC LIMIT 1) AS latest"
)

// EventsStore implements persistence.EventsStore on the events_store table
// of resources/eventstore_postgres.sql.
//
// An event's payload is stored as the Value of its Any and its manifest as
// the TypeUrl, so a read returns the Any that was written.
type EventsStore struct {
	// pool runs the statements. The application owns it.
	pool Pool
}

// enforce the interface implementation
var _ persistence.EventsStore = (*EventsStore)(nil)

// NewEventsStore creates an events store running on the given pool. The store
// never closes the pool, so the pool can be shared with the other stores and
// with the application.
func NewEventsStore(pool Pool) *EventsStore {
	return &EventsStore{pool: pool}
}

// Connect verifies the database is reachable.
func (s *EventsStore) Connect(ctx context.Context) error {
	return connect(ctx, s.pool)
}

// Disconnect releases nothing: the pool belongs to the application.
func (s *EventsStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the database is reachable.
func (s *EventsStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// WriteEvents inserts the events in one statement, so either every event is
// stored or none is. An event whose persistence ID and sequence number are
// stored already fails the write.
func (s *EventsStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	if len(events) == 0 {
		return nil
	}

	var (
		persistenceIDs   = make([]string, len(events))
		sequenceNumbers  = make([]int64, len(events))
		deletedFlags     = make([]bool, len(events))
		payloads         = make([][]byte, len(events))
		manifests        = make([]string, len(events))
		timestamps       = make([]int64, len(events))
		shardNumbers     = make([]int64, len(events))
		encryptionKeyIDs = make([]string, len(events))
		encryptedFlags   = make([]bool, len(events))
	)

	for index, event := range events {
		persistenceIDs[index] = event.GetPersistenceId()
		sequenceNumbers[index] = int64(event.GetSequenceNumber())
		deletedFlags[index] = event.GetIsDeleted()
		payloads[index] = payloadOf(event.GetEvent())
		manifests[index] = event.GetEvent().GetTypeUrl()
		timestamps[index] = event.GetTimestamp()
		shardNumbers[index] = int64(event.GetShard())
		encryptionKeyIDs[index] = event.GetEncryptionKeyId()
		encryptedFlags[index] = event.GetIsEncrypted()
	}

	if _, err := s.pool.Exec(ctx, insertEventsSQL,
		persistenceIDs, sequenceNumbers, deletedFlags, payloads, manifests,
		timestamps, shardNumbers, encryptionKeyIDs, encryptedFlags); err != nil {
		return fmt.Errorf("postgres: failed to write the events: %w", err)
	}

	return nil
}

// DeleteEvents deletes the events of the persistence ID up to the given
// sequence number, inclusive.
func (s *EventsStore) DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	if _, err := s.pool.Exec(ctx, deleteEventsSQL, persistenceID, int64(toSequenceNumber)); err != nil {
		return fmt.Errorf("postgres: failed to delete the events: %w", err)
	}

	return nil
}

// ReplayEvents returns at most limit events of the persistence ID between the
// two sequence numbers, both inclusive, in sequence order.
func (s *EventsStore) ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, limit uint64) ([]*egopb.Event, error) {
	rows, err := s.pool.Query(ctx, replayEventsSQL, persistenceID, int64(fromSequenceNumber), int64(toSequenceNumber), int64(limit))
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to replay the events: %w", err)
	}

	events, err := collectEvents(rows, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to replay the events: %w", err)
	}

	return events, nil
}

// GetLatestEvent returns the event of the persistence ID with the highest
// sequence number, or nil when it has none.
func (s *EventsStore) GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error) {
	event, err := scanEvent(s.pool.QueryRow(ctx, latestEventSQL, persistenceID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the latest event: %w", err)
	}

	return event, nil
}

// PersistenceIDs returns at most pageSize distinct persistence IDs after
// pageToken, in ascending order, and the token of the next page. The next
// token is empty once the last page is read.
func (s *EventsStore) PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) ([]string, string, error) {
	rows, err := s.pool.Query(ctx, persistenceIDsSQL, pageToken, int64(pageSize))
	if err != nil {
		return nil, "", fmt.Errorf("postgres: failed to list the persistence IDs: %w", err)
	}

	persistenceIDs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, "", fmt.Errorf("postgres: failed to list the persistence IDs: %w", err)
	}

	if len(persistenceIDs) == 0 || uint64(len(persistenceIDs)) < pageSize {
		return persistenceIDs, "", nil
	}

	return persistenceIDs, persistenceIDs[len(persistenceIDs)-1], nil
}

// EventsByShard returns at most limit events of the shard whose timestamp is
// at or after fromTimestamp, ordered by timestamp, then sequence number, then
// persistence ID.
func (s *EventsStore) EventsByShard(ctx context.Context, shardNumber uint64, fromTimestamp int64, limit uint64) ([]*egopb.Event, error) {
	rows, err := s.pool.Query(ctx, eventsByShardSQL, int64(shardNumber), fromTimestamp, int64(limit))
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the events of shard %d: %w", shardNumber, err)
	}

	events, err := collectEvents(rows, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the events of shard %d: %w", shardNumber, err)
	}

	return events, nil
}

// ShardOffsets returns every shard holding events mapped to the timestamp of
// its most recent event. An empty journal yields an empty map.
func (s *EventsStore) ShardOffsets(ctx context.Context) (map[uint64]int64, error) {
	rows, err := s.pool.Query(ctx, shardOffsetsSQL, int64(persistence.NumberOfShards-1))
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the shard offsets: %w", err)
	}

	offsets, err := collectTimestamps(rows)
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to read the shard offsets: %w", err)
	}

	return offsets, nil
}

// scanEvent reads an event from a row of eventColumns.
func scanEvent(row pgx.Row) (*egopb.Event, error) {
	event := &egopb.Event{Event: new(anypb.Any)}
	if err := row.Scan(
		&event.PersistenceId,
		&event.SequenceNumber,
		&event.IsDeleted,
		&event.Event.Value,
		&event.Event.TypeUrl,
		&event.Timestamp,
		&event.Shard,
		&event.EncryptionKeyId,
		&event.IsEncrypted,
	); err != nil {
		return nil, err
	}

	return event, nil
}

// collectEvents reads the events of rows, of which there are at most limit, and closes rows.
func collectEvents(rows pgx.Rows, limit uint64) ([]*egopb.Event, error) {
	return pgx.AppendRows(make([]*egopb.Event, 0, capacity(limit)), rows, func(row pgx.CollectableRow) (*egopb.Event, error) {
		return scanEvent(row)
	})
}

// collectTimestamps reads rows of a shard number and a timestamp into a map
// and closes rows.
func collectTimestamps(rows pgx.Rows) (map[uint64]int64, error) {
	timestamps := make(map[uint64]int64)

	var (
		shardNumber uint64
		timestamp   int64
	)

	if _, err := pgx.ForEachRow(rows, []any{&shardNumber, &timestamp}, func() error {
		timestamps[shardNumber] = timestamp
		return nil
	}); err != nil {
		return nil, err
	}

	return timestamps, nil
}

// payloadOf returns the Value of message, never nil: the payload columns are
// NOT NULL and a message with no field set has an empty Value.
func payloadOf(message *anypb.Any) []byte {
	if payload := message.GetValue(); payload != nil {
		return payload
	}

	return []byte{}
}
