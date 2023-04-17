package storage

import (
	"context"

	"github.com/tochemey/ego/egopb"
)

// EventsStore defines the store contract
// This helps implement any persistence store whether it is an RDBMS or No-SQL database
type EventsStore interface {
	// Connect connects to the journal store
	Connect(ctx context.Context) error
	// Disconnect disconnect the journal store
	Disconnect(ctx context.Context) error
	// WriteEvents persist store in batches for a given persistenceID.
	// Note: persistence id and the sequence number make a record in the journal store unique. Failure to ensure that
	// can lead to some un-wanted behaviors and data inconsistency
	WriteEvents(ctx context.Context, events []*egopb.Event) error
	// DeleteEvents deletes store from the store upt to a given sequence number (inclusive)
	DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error
	// ReplayEvents fetches store for a given persistence ID from a given sequence number(inclusive) to a given sequence number(inclusive) with a maximum of journals to be replayed.
	ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, max uint64) ([]*egopb.Event, error)
	// GetLatestEvent fetches the latest event
	GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error)
	// PersistenceIDs returns the distinct list of all the persistence ids in the journal store
	PersistenceIDs(ctx context.Context) (persistenceIDs []string, err error)
	// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
	Ping(ctx context.Context) error
}
