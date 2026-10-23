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

package projection

import (
	"context"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
)

// Handler processes the events a projection consumes from the events store,
// typically by materializing them into a read model (a database table, a
// cache, a search index, …).
//
// Concurrency: Handle may be invoked concurrently for events belonging to
// different shards, so implementations must be safe for concurrent use.
// Events of the same shard — and therefore of the same persistence ID — are
// always delivered sequentially, in order of their revision. In practice a
// handler whose only state is a concurrency-safe client (e.g. *pgxpool.Pool,
// *sql.DB) needs no extra synchronization; only mutable state held on the
// handler struct itself (in-memory maps, counters, batch buffers) must be
// guarded.
//
// Delivery: the projection commits its offset once per processed batch, so
// delivery is at-least-once — a crash mid-batch re-delivers the whole batch
// on restart. Handlers should therefore be idempotent. Two ways to make the
// effect of a redelivered event happen only once: write the read model with an
// upsert keyed by (persistenceID, revision), or keep the last applied revision
// of each persistence ID in the read model, updated in the same transaction as
// the read model, and skip any event at or below it. The second relies on the
// per-persistence-ID revision order above. For exactly-once effects in the
// database that holds the offset store, implement TransactionalHandler instead.
//
// Errors: a non-nil error triggers the projection's Recovery policy (fail,
// retry, or skip, optionally routing the event to the DeadLetterHandler).
// Panics raised by Handle are recovered by the runner and treated as errors.
type Handler interface {
	// Handle processes a single event consumed by the projection.
	//
	// persistenceID identifies the entity that emitted the event, event is the
	// (decrypted and schema-adapted) payload, and revision is the event's
	// sequence number within that entity's journal.
	Handle(ctx context.Context, persistenceID string, event *anypb.Any, revision uint64) error
}

// TransactionalHandler processes the events a projection consumes and records
// each event's offset in the same database transaction as the event's effect,
// so the effect is applied exactly once, across crashes, restarts and cluster
// failover. Set it with Options.TransactionalHandler instead of Options.Handler.
//
// Contract: Handle writes offset with the offset store's transactional write
// (for example a store method taking the open transaction) inside the
// transaction that applies the event, and commits both together. The offset
// store must therefore live in the database the handler writes to. When the
// transaction commits, the projection does not deliver the event again: on a
// restart it reads the offset back and skips the event. That holds while the
// projection keeps the entity's offsets. A reset deletes every offset of the
// projection, and the events after the reset point are delivered again:
// Options.ResetOffset, which is applied every time the projection starts,
// Engine.RebuildProjection and migration.ResumeProjection all reset. The
// projection also deletes an entity's offsets once they are a day behind its
// position on the shard and were recorded more than nine days ago; such an
// entity can have its earlier events delivered again when a later event
// reaches the projection before an earlier one it has not processed.
//
// When Handle returns an error, the transaction must be rolled back; the
// projection's recovery policy then decides whether the event is retried,
// skipped or stops the projection.
// A handler that commits without writing offset leaves the projection unable to
// recognize the event as processed, and it delivers the event again.
//
// Effects outside that database, such as HTTP calls or messages to a broker,
// are not covered and remain at-least-once.
//
// Concurrency and ordering are the same as for Handler.
type TransactionalHandler interface {
	// Handle processes a single event consumed by the projection and records
	// offset in the same transaction.
	//
	// persistenceID, event and revision are as for Handler.Handle. offset is
	// the row the projection records for this event: write it unchanged.
	Handle(ctx context.Context, persistenceID string, event *anypb.Any, revision uint64, offset *egopb.EntityOffset) error
}

// DiscardHandler implements the projection Handler interface
// This underlying really does nothing with the consumed event
// Note: this will be useful when writing unit tests
type DiscardHandler struct {
}

// enforce the complete implementation of the Handler interface
var _ Handler = (*DiscardHandler)(nil)

// NewDiscardHandler creates an instance of DiscardHandler
func NewDiscardHandler() *DiscardHandler {
	return &DiscardHandler{}
}

// Handle handles the events consumed
// nolint
func (x *DiscardHandler) Handle(_ context.Context, _ string, _ *anypb.Any, _ uint64) error {
	return nil
}
