package offsetstore

import (
	"context"

	"github.com/tochemey/ego/egopb"
)

// OffsetStore defines the contract needed to persist offsets
type OffsetStore interface {
	// Connect connects to the offset store
	Connect(ctx context.Context) error
	// Disconnect disconnects the offset store
	Disconnect(ctx context.Context) error
	// WriteOffset writes the current offset of the event consumed for a given projection ID
	// Note: persistence id and the projection name make a record in the journal store unique. Failure to ensure that
	// can lead to some un-wanted behaviors and data inconsistency
	WriteOffset(ctx context.Context, offset *egopb.Offset) error
	// GetCurrentOffset returns the current offset of a given projection ID
	GetCurrentOffset(ctx context.Context, projectionID *ProjectionID) (current *egopb.Offset, err error)
	// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
	Ping(ctx context.Context) error
}
