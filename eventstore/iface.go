/*
 * Copyright (c) 2022-2023 Tochemey
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

package eventstore

import (
	"context"

	"github.com/tochemey/ego/egopb"
)

// EventsStore defines the API to write to the events store
type EventsStore interface {
	// Connect connects to the journal store
	Connect(ctx context.Context) error
	// Disconnect disconnect the journal store
	Disconnect(ctx context.Context) error
	// WriteEvents persist store in batches for a given persistenceID.
	// Note: persistence id and the sequence number make a record in the journal store unique. Failure to ensure that
	// can lead to some un-wanted behaviors and data inconsistency
	WriteEvents(ctx context.Context, events []*egopb.Event) error
	// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
	Ping(ctx context.Context) error
	// DeleteEvents deletes store from the store upt to a given sequence number (inclusive)
	DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error
	// ReplayEvents fetches store for a given persistence ID from a given sequence number(inclusive) to a given sequence number(inclusive) with a maximum of journals to be replayed.
	ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, max uint64) ([]*egopb.Event, error)
	// GetLatestEvent fetches the latest event
	GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error)
	// PersistenceIDs returns the distinct list of all the persistence ids in the journal store
	PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error)
	// GetShardEvents returns the next (max) events after the offset in the journal for a given shard
	GetShardEvents(ctx context.Context, shardNumber uint64, offset uint64, max uint64) (events []*egopb.Event, err error)
}
