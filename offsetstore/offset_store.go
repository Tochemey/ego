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

package offsetstore

import (
	"context"

	"github.com/tochemey/ego/v5/egopb"
)

// OffsetStore persists how far each projection has processed the journal.
//
// A projection records one offset per persistence ID it processes: the sequence
// number of the last event it processed and that event's timestamp (see
// egopb.EntityOffset). A projection's position on a shard is the newest
// timestamp among its offsets on that shard. The offsets of the recent past let
// a restarted projection recognize the events it has already processed, and
// detect an event of a persistence ID that is missing.
//
// SQL-backed stores keep the offsets in a table keyed by
// (projection_name, shard_number, timestamp, persistence_id, sequence_number),
// plus one row per projection for the start offset ResetOffset records.
type OffsetStore interface {
	// Connect connects to the offset store
	Connect(ctx context.Context) error
	// Disconnect disconnects the offset store
	Disconnect(ctx context.Context) error
	// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
	Ping(ctx context.Context) error
	// WriteOffsets stores the given offsets of the projection in one atomic write: either every
	// offset is stored or none is. Every offset is a new row; earlier offsets of the same
	// persistence ID are kept until DeleteOffsetsBefore removes them. Writing an offset that is
	// stored already is not an error, and it stays stored once: SQL-backed stores insert with
	// ON CONFLICT DO NOTHING.
	WriteOffsets(ctx context.Context, projectionName string, offsets []*egopb.EntityOffset) error
	// ReadOffsets returns at most limit offsets of the projection on the given shard, the newest
	// timestamp first. A projection with no offset on the shard yields an empty result.
	ReadOffsets(ctx context.Context, projectionName string, shardNumber uint64, limit uint64) ([]*egopb.EntityOffset, error)
	// ReadEntityOffset returns the offset of the persistence ID with the highest sequence number,
	// or nil when the projection has no offset for it.
	ReadEntityOffset(ctx context.Context, projectionName string, shardNumber uint64, persistenceID string) (*egopb.EntityOffset, error)
	// LatestOffsets returns, for every shard the projection has an offset on, the newest timestamp
	// among its offsets on that shard. SQL-backed stores implement it with a single query:
	//
	//	SELECT shard_number, MAX(timestamp) FROM offsets_store
	//	WHERE projection_name = $1 GROUP BY shard_number
	LatestOffsets(ctx context.Context, projectionName string) (map[uint64]int64, error)
	// DeleteOffsetsBefore deletes the offsets of the projection on the given shard whose timestamp
	// is before the given timestamp and that were consumed before consumedBefore. Both bounds are in
	// Unix nanoseconds. The projection only passes a timestamp older than its newest offset on the
	// shard, so that offset always remains.
	DeleteOffsetsBefore(ctx context.Context, projectionName string, shardNumber uint64, before, consumedBefore int64) error
	// ResetOffset deletes every offset of the projection and records value, a timestamp in Unix
	// nanoseconds, as the point the projection starts from on every shard.
	ResetOffset(ctx context.Context, projectionName string, value int64) error
	// StartOffset returns the timestamp the latest ResetOffset recorded for the projection, or zero
	// when there is none.
	StartOffset(ctx context.Context, projectionName string) (int64, error)
	// DeleteOffset deletes every offset and the start offset of the projection. Deleting the
	// offsets of a projection that has none is not an error.
	DeleteOffset(ctx context.Context, projectionName string) error
}
