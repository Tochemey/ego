package memory

import (
	"context"
	"fmt"
	"sort"

	"github.com/tochemey/ego/storage"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/tochemey/ego/egopb"
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

// make sure the PostgresEventStore implements the EventsStore interface
var _ storage.EventsStore = &EventsStore{}

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
	ctx, span := telemetry.SpanContext(ctx, "EventsStore.Connect")
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
	ctx, span := telemetry.SpanContext(ctx, "EventsStore.Disconnect")
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
	ctx, span := telemetry.SpanContext(ctx, "EventsStore.Ping")
	defer span.End()

	// check whether we are connected or not
	if !s.connected.Load() {
		return s.Connect(ctx)
	}

	return nil
}

// PersistenceIDs returns the distinct list of all the persistence ids in the journal store
func (s *EventsStore) PersistenceIDs(ctx context.Context) (persistenceIDs []string, err error) {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "EventsStore.PersistenceIDs")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil, errors.New("journal store is not connected")
	}

	// spawn a db transaction for read-only
	txn := s.db.Txn(false)
	defer txn.Abort()

	// fetch all the records
	it, err := txn.Get(journalTableName, persistenceIDIndex)
	// handle the error
	if err != nil {
		return nil, errors.Wrap(err, "failed to get the persistence Ids")
	}

	var journals []*journal
	for row := it.Next(); row != nil; row = it.Next() {
		if journal, ok := row.(*journal); ok {
			journals = append(journals, journal)
		}
	}

	persistenceIDs = make([]string, len(journals))
	for i, journal := range journals {
		persistenceIDs[i] = journal.PersistenceID
	}

	return
}

// WriteEvents persist events in batches for a given persistenceID
func (s *EventsStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "EventsStore.WriteEvents")
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
// FIXME: enhance the implementation. As it stands it may be a bit slow
func (s *EventsStore) DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "EventsStore.DeleteEvents")
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
	ctx, span := telemetry.SpanContext(ctx, "EventsStore.ReplayEvents")
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
	ctx, span := telemetry.SpanContext(ctx, "EventsStore.GetLatestEvent")
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
		if err == memdb.ErrNotFound {
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
		}, nil
	}

	return nil, fmt.Errorf("failed to fetch the latest event from the database for persistenceId=%s", persistenceID)
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
