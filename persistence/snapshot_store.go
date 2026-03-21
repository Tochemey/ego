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

	"github.com/tochemey/ego/v4/egopb"
)

// SnapshotStore defines the API to persist and retrieve entity state snapshots.
// Snapshots are used to speed up recovery of event-sourced entities by avoiding
// full event replay from the beginning of time.
type SnapshotStore interface {
	// Connect connects to the snapshot store
	Connect(ctx context.Context) error
	// Disconnect disconnects the snapshot store
	Disconnect(ctx context.Context) error
	// Ping verifies a connection to the snapshot store is still alive, establishing a connection if necessary.
	Ping(ctx context.Context) error
	// WriteSnapshot persists a snapshot for a given persistenceID.
	WriteSnapshot(ctx context.Context, snapshot *egopb.Snapshot) error
	// GetLatestSnapshot fetches the latest snapshot for a given persistenceID.
	// Returns nil when no snapshot is found.
	GetLatestSnapshot(ctx context.Context, persistenceID string) (*egopb.Snapshot, error)
	// DeleteSnapshots deletes all snapshots for a given persistenceID up to a given sequence number (inclusive).
	DeleteSnapshots(ctx context.Context, persistenceID string, toSequenceNumber uint64) error
}
