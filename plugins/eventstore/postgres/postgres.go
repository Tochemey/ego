/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package postgres

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/internal/postgres"
	"github.com/tochemey/ego/v3/persistence"
)

var (
	columns = []string{
		"persistence_id",
		"sequence_number",
		"is_deleted",
		"event_payload",
		"event_manifest",
		"state_payload",
		"state_manifest",
		"timestamp",
		"shard_number",
	}

	tableName = "events_store"
)

// EventsStore implements the EventsStore interface
// and helps persist events in a Postgres database
type EventsStore struct {
	db postgres.Postgres
	sb sq.StatementBuilderType
	// insertBatchSize represents the chunk of data to bulk insert.
	// This helps avoid the postgres 65535 parameter limit.
	// This is necessary because Postgres uses a 32-bit int for binding input parameters and
	// is not able to track anything larger.
	// Note: Change this value when you know the size of data to bulk insert at once. Otherwise, you
	// might encounter the postgres 65535 parameter limit error.
	insertBatchSize int
	// hold the connection state to avoid multiple connection of the same instance
	connected *atomic.Bool
}

// enforce interface implementation
var _ persistence.EventsStore = (*EventsStore)(nil)

// NewEventsStore creates a new instance of PostgresEventStore
func NewEventsStore(config *Config) *EventsStore {
	// create the underlying db connection
	db := postgres.New(postgres.NewConfig(config.DBHost, config.DBPort, config.DBUser, config.DBPassword, config.DBName))
	return &EventsStore{
		db:              db,
		sb:              sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
		insertBatchSize: 500,
		connected:       atomic.NewBool(false),
	}
}

// Connect connects to the underlying postgres database
func (s *EventsStore) Connect(ctx context.Context) error {
	// check whether this instance of the journal is connected or not
	if s.connected.Load() {
		return nil
	}

	// connect to the underlying db
	if err := s.db.Connect(ctx); err != nil {
		return err
	}

	// set the connection status
	s.connected.Store(true)

	return nil
}

// Disconnect disconnects from the underlying postgres database
func (s *EventsStore) Disconnect(ctx context.Context) error {
	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil
	}

	// disconnect the underlying database
	if err := s.db.Disconnect(ctx); err != nil {
		return err
	}
	// set the connection status
	s.connected.Store(false)

	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (s *EventsStore) Ping(ctx context.Context) error {
	// check whether we are connected or not
	if !s.connected.Load() {
		return s.Connect(ctx)
	}

	return nil
}

// PersistenceIDs returns the distinct list of all the persistence ids in the journal store
func (s *EventsStore) PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error) {
	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, "", errors.New("journal store is not connected")
	}

	// create the database delete statement
	statement := s.sb.
		Select("persistence_id").
		Distinct().
		From(tableName).
		Limit(pageSize).
		OrderBy("persistence_id ASC")

	// set the page token
	if pageToken != "" {
		statement = statement.Where(sq.Gt{"persistence_id": pageToken})
	}

	// get the sql statement and the arguments
	query, args, err := statement.ToSql()
	// handle the error
	if err != nil {
		return nil, "", fmt.Errorf("failed to build the sql statement: %w", err)
	}

	// create the ds to hold the database record
	type row struct {
		PersistenceID string
	}

	// execute the query against the database
	var rows []*row
	err = s.db.SelectAll(ctx, &rows, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch the events from the database: %w", err)
	}

	// grab the fetched records
	persistenceIDs = make([]string, len(rows))
	for index, row := range rows {
		persistenceIDs[index] = row.PersistenceID
	}

	// set the next page token
	nextPageToken = persistenceIDs[len(persistenceIDs)-1]

	return
}

// WriteEvents writes a bunch of events into the underlying postgres database
func (s *EventsStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return errors.New("journal store is not connected")
	}

	// check whether the journals list is empty
	if len(events) == 0 {
		// do nothing
		return nil
	}

	// let us begin a database transaction to make sure we atomically write those events into the database
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	// return the error in case we are unable to get a database transaction
	if err != nil {
		return fmt.Errorf("failed to obtain a database transaction: %w", err)
	}

	// start creating the sql statement for insertion
	statement := s.sb.Insert(tableName).Columns(columns...)
	for index, event := range events {
		var (
			eventManifest string
			eventBytes    []byte
			stateManifest string
			stateBytes    []byte
		)

		// serialize the event and resulting state
		eventBytes, _ = proto.Marshal(event.GetEvent())
		stateBytes, _ = proto.Marshal(event.GetResultingState())

		// grab the manifest
		eventManifest = string(event.GetEvent().ProtoReflect().Descriptor().FullName())
		stateManifest = string(event.GetResultingState().ProtoReflect().Descriptor().FullName())

		// build the insertion values
		statement = statement.Values(
			event.GetPersistenceId(),
			event.GetSequenceNumber(),
			event.GetIsDeleted(),
			eventBytes,
			eventManifest,
			stateBytes,
			stateManifest,
			event.GetTimestamp(),
			event.GetShard(),
		)

		if (index+1)%s.insertBatchSize == 0 || index == len(events)-1 {
			// get the SQL statement to run
			query, args, err := statement.ToSql()
			// handle the error while generating the SQL
			if err != nil {
				return fmt.Errorf("unable to build sql insert statement: %w", err)
			}
			// insert into the table
			_, execErr := tx.Exec(ctx, query, args...)
			if execErr != nil {
				// attempt to roll back the transaction and log the error in case there is an error
				if err = tx.Rollback(ctx); err != nil {
					return fmt.Errorf("unable to rollback db transaction: %w", err)
				}
				// return the main error
				return fmt.Errorf("failed to record events: %w", execErr)
			}

			// reset the statement for the next bulk
			statement = s.sb.Insert(tableName).Columns(columns...)
		}
	}

	// commit the transaction
	if commitErr := tx.Commit(ctx); commitErr != nil {
		// return the commit error in case there is one
		return fmt.Errorf("failed to record events: %w", commitErr)
	}
	// every looks good
	return nil
}

// DeleteEvents deletes events from the postgres up to a given sequence number (inclusive)
func (s *EventsStore) DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return errors.New("journal store is not connected")
	}

	// create the database delete statement
	statement := s.sb.
		Delete(tableName).
		Where(sq.Eq{"persistence_id": persistenceID}).
		Where(sq.LtOrEq{"sequence_number": toSequenceNumber})

	// get the sql statement and the arguments
	query, args, err := statement.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build the delete events sql statement: %w", err)
	}

	// execute the sql statement
	if _, err := s.db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to delete events from the database: %w", err)
	}

	return nil
}

// ReplayEvents fetches events for a given persistence ID from a given sequence number(inclusive) to a given sequence number(inclusive)
func (s *EventsStore) ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, max uint64) ([]*egopb.Event, error) {
	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, errors.New("journal store is not connected")
	}

	// create the database select statement
	statement := s.sb.
		Select(columns...).
		From(tableName).
		Where(sq.Eq{"persistence_id": persistenceID}).
		Where(sq.GtOrEq{"sequence_number": fromSequenceNumber}).
		Where(sq.LtOrEq{"sequence_number": toSequenceNumber}).
		OrderBy("sequence_number ASC").
		Limit(max)

	// get the sql statement and the arguments
	query, args, err := statement.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build the select sql statement: %w", err)
	}

	// execute the query against the database
	var rows rows
	err = s.db.SelectAll(ctx, &rows, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the events from the database: %w", err)
	}

	// return the derivative events
	return rows.ToEvents()
}

// GetLatestEvent fetches the latest event
func (s *EventsStore) GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error) {
	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, errors.New("journal store is not connected")
	}

	// create the database select statement
	statement := s.sb.
		Select(columns...).
		From(tableName).
		Where(sq.Eq{"persistence_id": persistenceID}).
		OrderBy("sequence_number DESC").
		Limit(1)

	// get the sql statement and the arguments
	query, args, err := statement.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build the select sql statement: %w", err)
	}

	// execute the query against the database
	row := new(row)
	err = s.db.Select(ctx, row, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the latest event from the database: %w", err)
	}

	// check whether we do have data
	if row.PersistenceID == "" {
		return nil, nil
	}

	// return the derivative event
	return row.ToEvent()
}

// GetShardEvents returns the next (max) events after the offset in the journal for a given shard
func (s *EventsStore) GetShardEvents(ctx context.Context, shardNumber uint64, offset int64, max uint64) ([]*egopb.Event, int64, error) {
	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, 0, errors.New("journal store is not connected")
	}

	// create the database select statement
	statement := s.sb.
		Select(columns...).
		From(tableName).
		Where(sq.Eq{"shard_number": shardNumber}).
		Where(sq.Gt{"timestamp": offset}).
		OrderBy("timestamp ASC").
		Limit(max)

	// get the sql statement and the arguments
	query, args, err := statement.ToSql()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to build the select sql statement: %w", err)
	}

	// execute the query against the database
	var rows rows
	err = s.db.SelectAll(ctx, &rows, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch the events from the database: %w", err)
	}

	// short-circuit the request
	if len(rows) == 0 {
		return nil, 0, nil
	}

	// grab the events
	events, err := rows.ToEvents()
	// handle the error when parsing
	if err != nil {
		return nil, 0, err
	}
	// get the next offset
	nextOffset := events[len(events)-1].GetTimestamp()
	// return the data
	return events, nextOffset, nil
}

// ShardNumbers returns the distinct list of all the shards in the journal store
func (s *EventsStore) ShardNumbers(ctx context.Context) ([]uint64, error) {
	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, errors.New("journal store is not connected")
	}

	// create the statement
	statement := s.sb.
		Select("DISTINCT shard_number").
		From(tableName)

	// get the sql statement and the arguments
	query, args, err := statement.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build the select sql statement: %w", err)
	}

	var shardNumbers []uint64
	err = s.db.SelectAll(ctx, &shardNumbers, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the events from the database: %w", err)
	}

	return shardNumbers, nil
}
