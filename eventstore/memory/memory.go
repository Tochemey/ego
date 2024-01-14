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

package memory

import (
	"context"
	"fmt"
	"sort"

	goset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/eventstore"
	"github.com/tochemey/ego/internal/telemetry"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
)

// EventsStore keep in memory every journal
// NOTE: NOT RECOMMENDED FOR PRODUCTION CODE because all records are in memory and there is no durability.
// This is recommended for tests or PoC
type EventsStore struct {
	// specifies the underlying database
	db *memdb.MemDB
	// this is only useful for tests
	KeepRecordsAfterDisconnect bool
	// hold the connection state to avoid multiple connection of the same instance
	connected *atomic.Bool
}

// enforce interface implementation
var _ eventstore.EventsStore = (*EventsStore)(nil)

// NewEventsStore creates a new instance of EventsStore
func NewEventsStore() *EventsStore {
	return &EventsStore{
		KeepRecordsAfterDisconnect: false,
		connected:                  atomic.NewBool(false),
	}
}

// Connect connects to the journal store
func (s *EventsStore) Connect(ctx context.Context) error {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "eventsStore.Connect")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if s.connected.Load() {
		return nil
	}

	// create an instance of the database
	db, err := memdb.NewMemDB(journalSchema)
	// handle the eventual error
	if err != nil {
		return err
	}
	// set the journal store underlying database
	s.db = db

	// set the connection status
	s.connected.Store(true)

	return nil
}

// Disconnect disconnect the journal store
func (s *EventsStore) Disconnect(ctx context.Context) error {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "eventsStore.Disconnect")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil
	}

	// clear all records
	if !s.KeepRecordsAfterDisconnect {
		// spawn a db transaction for read-only
		txn := s.db.Txn(true)

		// free memory resource
		if _, err := txn.DeleteAll(journalTableName, journalPK); err != nil {
			txn.Abort()
			return errors.Wrap(err, "failed to free memory resource")
		}
		txn.Commit()
	}

	// set the connection status
	s.connected.Store(false)

	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (s *EventsStore) Ping(ctx context.Context) error {
	// add a span context
	spanCtx, span := telemetry.SpanContext(ctx, "eventsStore.Ping")
	defer span.End()

	// check whether we are connected or not
	if !s.connected.Load() {
		return s.Connect(spanCtx)
	}

	return nil
}

// PersistenceIDs returns the distinct list of all the persistence ids in the journal store
// FIXME: enhance the implementation. As it stands it will be a bit slow when there are a lot of records
func (s *EventsStore) PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error) {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "eventsStore.PersistenceIDs")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, "", errors.New("journal store is not connected")
	}

	// spawn a db transaction for read-only
	txn := s.db.Txn(false)
	defer txn.Abort()

	// define the records iterator and error variables
	var (
		it memdb.ResultIterator
	)

	// check whether the page token is set
	if pageToken != "" {
		// execute the query to fetch the records
		it, err = txn.LowerBound(journalTableName, persistenceIDIndex, pageToken)
	} else {
		// fetch all the records. default behavior
		it, err = txn.Get(journalTableName, persistenceIDIndex)
	}

	// handle the error
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to get the persistence Ids")
	}

	var journals []*journal
	// fetch the records
	for row := it.Next(); row != nil; row = it.Next() {
		// check whether we have reached the page size
		if len(journals) == int(pageSize) {
			break
		}
		// parse the next record
		if journal, ok := row.(*journal); ok {
			journals = append(journals, journal)
		}
	}

	// build the persistence ids fetched
	// iterate the records that have been fetched earlier
	for _, journal := range journals {
		// TODO: refactor this cowboy code
		// skip the page token record
		if journal.PersistenceID == pageToken {
			continue
		}
		// grab the id
		persistenceIDs = append(persistenceIDs, journal.PersistenceID)
	}

	// short-circuit when there are no records
	if len(persistenceIDs) == 0 {
		return nil, "", nil
	}

	// let us sort the fetch ids
	sort.SliceStable(persistenceIDs, func(i, j int) bool {
		return persistenceIDs[i] <= persistenceIDs[j]
	})

	// set the next page token
	nextPageToken = persistenceIDs[len(persistenceIDs)-1]

	return
}

// WriteEvents persist events in batches for a given persistenceID
func (s *EventsStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "eventsStore.WriteEvents")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return errors.New("journal store is not connected")
	}

	// spawn a db transaction
	txn := s.db.Txn(true)
	// iterate the event and persist the record
	for _, event := range events {
		// serialize the event and resulting state
		eventBytes, _ := proto.Marshal(event.GetEvent())
		stateBytes, _ := proto.Marshal(event.GetResultingState())

		// grab the manifest
		eventManifest := string(event.GetEvent().ProtoReflect().Descriptor().FullName())
		stateManifest := string(event.GetResultingState().ProtoReflect().Descriptor().FullName())

		// create an instance of Journal
		journal := &journal{
			Ordering:       uuid.NewString(),
			PersistenceID:  event.GetPersistenceId(),
			SequenceNumber: event.GetSequenceNumber(),
			IsDeleted:      event.GetIsDeleted(),
			EventPayload:   eventBytes,
			EventManifest:  eventManifest,
			StatePayload:   stateBytes,
			StateManifest:  stateManifest,
			Timestamp:      event.GetTimestamp(),
			ShardNumber:    event.GetShard(),
		}

		// persist the record
		if err := txn.Insert(journalTableName, journal); err != nil {
			// abort the transaction
			txn.Abort()
			// return the error
			return errors.Wrap(err, "failed to persist event on to the journal store")
		}
	}
	// commit the transaction
	txn.Commit()

	return nil
}

// DeleteEvents deletes events from the store upt to a given sequence number (inclusive)
// FIXME: enhance the implementation. As it stands it may be a bit slow when there are a lot of records
func (s *EventsStore) DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "eventsStore.DeleteEvents")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return errors.New("journal store is not connected")
	}

	// spawn a db transaction for read-only
	txn := s.db.Txn(false)
	// fetch all the records that are not deleted and filter them out
	it, err := txn.Get(journalTableName, persistenceIDIndex, persistenceID)
	// handle the error
	if err != nil {
		// abort the transaction
		txn.Abort()
		return errors.Wrapf(err, "failed to delete %d persistenceId=%s events", toSequenceNumber, persistenceID)
	}

	// loop over the records and delete them
	var journals []*journal
	for row := it.Next(); row != nil; row = it.Next() {
		if journal, ok := row.(*journal); ok {
			journals = append(journals, journal)
		}
	}
	//  let us abort the transaction after fetching the matching records
	txn.Abort()

	// now let us delete the records whose sequence number are less or equal to the given sequence number
	// spawn a db transaction for write-only
	txn = s.db.Txn(true)

	// iterate over the records and delete them
	// TODO enhance this operation using the DeleteAll feature
	for _, journal := range journals {
		if journal.SequenceNumber <= toSequenceNumber {
			// delete that record
			if err := txn.Delete(journalTableName, journal); err != nil {
				// abort the transaction
				txn.Abort()
				return errors.Wrapf(err, "failed to delete %d persistenceId=%s events", toSequenceNumber, persistenceID)
			}
		}
	}
	// commit the transaction
	txn.Commit()
	return nil
}

// ReplayEvents fetches events for a given persistence ID from a given sequence number(inclusive) to a given sequence number(inclusive)
func (s *EventsStore) ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, max uint64) ([]*egopb.Event, error) {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "eventsStore.ReplayEvents")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, errors.New("journal store is not connected")
	}

	// spawn a db transaction for read-only
	txn := s.db.Txn(false)
	// fetch all the records for the given persistence ID
	it, err := txn.Get(journalTableName, persistenceIDIndex, persistenceID)
	// handle the error
	if err != nil {
		// abort the transaction
		txn.Abort()
		return nil, errors.Wrapf(err, "failed to replay events %d for persistenceId=%s events", (toSequenceNumber-fromSequenceNumber)+1, persistenceID)
	}

	// loop over the records and delete them
	var journals []*journal
	for row := it.Next(); row != nil; row = it.Next() {
		if journal, ok := row.(*journal); ok {
			journals = append(journals, journal)
		}
	}
	//  let us abort the transaction after fetching the matching records
	txn.Abort()

	// short circuit the operation when there are no records
	if len(journals) == 0 {
		return nil, nil
	}

	var events []*egopb.Event
	for _, journal := range journals {
		if journal.SequenceNumber >= fromSequenceNumber && journal.SequenceNumber <= toSequenceNumber {
			// unmarshal the event and the state
			evt, err := toProto(journal.EventManifest, journal.EventPayload)
			if err != nil {
				return nil, errors.Wrap(err, "failed to unmarshal the journal event")
			}
			state, err := toProto(journal.StateManifest, journal.StatePayload)
			if err != nil {
				return nil, errors.Wrap(err, "failed to unmarshal the journal state")
			}

			if uint64(len(events)) <= max {
				// create the event and add it to the list of events
				events = append(events, &egopb.Event{
					PersistenceId:  journal.PersistenceID,
					SequenceNumber: journal.SequenceNumber,
					IsDeleted:      journal.IsDeleted,
					Event:          evt,
					ResultingState: state,
					Timestamp:      journal.Timestamp,
					Shard:          journal.ShardNumber,
				})
			}
		}
	}

	// sort the subset by sequence number
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].GetSequenceNumber() < events[j].GetSequenceNumber()
	})

	return events, nil
}

// GetLatestEvent fetches the latest event
func (s *EventsStore) GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error) {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "eventsStore.GetLatestEvent")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, errors.New("journal store is not connected")
	}

	// spawn a db transaction for read-only
	txn := s.db.Txn(false)
	defer txn.Abort()
	// let us fetch the last record
	raw, err := txn.Last(journalTableName, persistenceIDIndex, persistenceID)
	if err != nil {
		// if the error is not found then return nil
		if errors.Is(err, memdb.ErrNotFound) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to fetch the latest event from the database for persistenceId=%s", persistenceID)
	}

	// no record found
	if raw == nil {
		return nil, nil
	}

	// let us cast the raw data
	if journal, ok := raw.(*journal); ok {
		// unmarshal the event and the state
		evt, err := toProto(journal.EventManifest, journal.EventPayload)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal the journal event")
		}
		state, err := toProto(journal.StateManifest, journal.StatePayload)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal the journal state")
		}

		return &egopb.Event{
			PersistenceId:  journal.PersistenceID,
			SequenceNumber: journal.SequenceNumber,
			IsDeleted:      journal.IsDeleted,
			Event:          evt,
			ResultingState: state,
			Timestamp:      journal.Timestamp,
			Shard:          journal.ShardNumber,
		}, nil
	}

	return nil, fmt.Errorf("failed to fetch the latest event from the database for persistenceId=%s", persistenceID)
}

// GetShardEvents returns the next (max) events after the offset in the journal for a given shard
func (s *EventsStore) GetShardEvents(ctx context.Context, shardNumber uint64, offset int64, max uint64) ([]*egopb.Event, int64, error) {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "eventsStore.GetShardEvents")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, 0, errors.New("journal store is not connected")
	}

	// spawn a db transaction for read-only
	txn := s.db.Txn(false)
	// fetch all the records for the given shard
	it, err := txn.Get(journalTableName, persistenceIDIndex)
	// handle the error
	if err != nil {
		// abort the transaction
		txn.Abort()
		return nil, 0, errors.Wrapf(err, "failed to get events of shard=(%d)", shardNumber)
	}

	// loop over the records and delete them
	var journals []*journal
	for row := it.Next(); row != nil; row = it.Next() {
		// cast the elt into the journal
		if journal, ok := row.(*journal); ok {
			// filter out the journal of the given shard number
			if journal.ShardNumber == shardNumber {
				journals = append(journals, journal)
			}
		}
	}
	//  let us abort the transaction after fetching the matching records
	txn.Abort()

	// short circuit the operation when there are no records
	if len(journals) == 0 {
		return nil, 0, nil
	}

	var events []*egopb.Event
	for _, journal := range journals {
		// only fetch record which timestamp is greater than the offset
		if journal.Timestamp > offset {
			// unmarshal the event and the state
			evt, err := toProto(journal.EventManifest, journal.EventPayload)
			if err != nil {
				return nil, 0, errors.Wrap(err, "failed to unmarshal the journal event")
			}
			state, err := toProto(journal.StateManifest, journal.StatePayload)
			if err != nil {
				return nil, 0, errors.Wrap(err, "failed to unmarshal the journal state")
			}

			if uint64(len(events)) <= max {
				// create the event and add it to the list of events
				events = append(events, &egopb.Event{
					PersistenceId:  journal.PersistenceID,
					SequenceNumber: journal.SequenceNumber,
					IsDeleted:      journal.IsDeleted,
					Event:          evt,
					ResultingState: state,
					Timestamp:      journal.Timestamp,
					Shard:          journal.ShardNumber,
				})
			}
		}
	}

	// short circuit the operation when there are no records
	if len(events) == 0 {
		return nil, 0, nil
	}

	// sort the subset by timestamp
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].GetTimestamp() <= events[j].GetTimestamp()
	})

	// grab the next offset
	nextOffset := events[len(events)-1].GetTimestamp()

	return events, nextOffset, nil
}

// ShardNumbers returns the distinct list of all the shards in the journal store
func (s *EventsStore) ShardNumbers(ctx context.Context) ([]uint64, error) {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "eventsStore.NumShards")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, errors.New("journal store is not connected")
	}

	// spawn a db transaction for read-only
	txn := s.db.Txn(false)
	// fetch all the records
	it, err := txn.Get(journalTableName, persistenceIDIndex)
	// handle the error
	if err != nil {
		// abort the transaction
		txn.Abort()
		return nil, errors.Wrap(err, "failed to fetch the list of shard number")
	}

	// loop over the records
	var journals []*journal
	for row := it.Next(); row != nil; row = it.Next() {
		if journal, ok := row.(*journal); ok {
			journals = append(journals, journal)
		}
	}
	//  let us abort the transaction after fetching the matching records
	txn.Abort()

	// short circuit the operation when there are no records
	if len(journals) == 0 {
		return nil, nil
	}

	// create a set to hold the unique list of shard numbers
	shards := goset.NewSet[uint64]()
	// iterate the list of journals and extract the shard numbers
	for _, journal := range journals {
		shards.Add(journal.ShardNumber)
	}

	// return the list
	return shards.ToSlice(), nil
}

// toProto converts a byte array given its manifest into a valid proto message
func toProto(manifest string, bytea []byte) (*anypb.Any, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(manifest))
	if err != nil {
		return nil, err
	}

	pm := mt.New().Interface()
	err = proto.Unmarshal(bytea, pm)
	if err != nil {
		return nil, err
	}

	if cast, ok := pm.(*anypb.Any); ok {
		return cast, nil
	}
	return nil, fmt.Errorf("failed to unpack message=%s", manifest)
}
