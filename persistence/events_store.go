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

package persistence

import (
	"context"

	"github.com/tochemey/ego/v5/egopb"
)

// EventsStore defines the API to write to the events store
type EventsStore interface {
	// Connect connects to the journal store
	Connect(ctx context.Context) error
	// Disconnect disconnect the journal store
	Disconnect(ctx context.Context) error
	// WriteEvents persists the events of one write of a persistence ID.
	//
	// The write is atomic: either every event is stored or none is. The persistence ID and the
	// sequence number identify an event, and WriteEvents must fail, storing nothing, when any of
	// the events is already stored. An entity has a single writer, so its sequence numbers have no
	// gaps; a second writer, for instance during a network partition, is rejected instead of
	// silently replacing events projections may have consumed already.
	WriteEvents(ctx context.Context, events []*egopb.Event) error
	// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
	Ping(ctx context.Context) error
	// DeleteEvents deletes store from the store up to a given sequence number (inclusive)
	DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error
	// ReplayEvents fetches store for a given persistence ID from a given sequence number(inclusive) to a given sequence number(inclusive) with a maximum of journals to be replayed.
	ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, limit uint64) ([]*egopb.Event, error)
	// GetLatestEvent fetches the latest event
	GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error)
	// PersistenceIDs returns the distinct list of all the persistence ids in the journal store
	PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error)
	// EventsByShard returns at most limit events of the given shard whose timestamp is at or after
	// fromTimestamp, ordered by timestamp, then by sequence number, then by persistence ID, so
	// that a limited read is the same page every time. SQL-backed stores implement it with a
	// single query:
	//
	//	SELECT ... FROM events_store
	//	WHERE shard_number = $1 AND timestamp >= $2
	//	ORDER BY timestamp, sequence_number, persistence_id
	//	LIMIT $3
	//
	// The lower bound is inclusive: several events can share a timestamp, and a projection that
	// stopped among them resumes at that timestamp and skips those it has already processed.
	EventsByShard(ctx context.Context, shardNumber uint64, fromTimestamp int64, limit uint64) ([]*egopb.Event, error)
	// ShardOffsets returns every distinct shard in the journal mapped to the
	// offset (timestamp) of its most recent event. Compared against the
	// offsets a projection has committed, it tells which shards have pending
	// events without scanning every shard. An empty journal yields an empty
	// map. SQL-backed stores implement it with a single query:
	//
	//	SELECT shard_number, MAX(timestamp) FROM events_store GROUP BY shard_number
	ShardOffsets(ctx context.Context) (map[uint64]int64, error)
}
