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

package ego

// RetentionPolicy controls how old events and snapshots are cleaned up after
// a snapshot has been successfully written. This prevents unbounded storage growth
// in production event-sourced systems.
type RetentionPolicy struct {
	// DeleteEventsOnSnapshot when true, deletes events up to the snapshot
	// sequence number after a successful snapshot write.
	DeleteEventsOnSnapshot bool
	// DeleteSnapshotsOnSnapshot when true, deletes older snapshots after
	// a new snapshot is written, keeping only the latest.
	DeleteSnapshotsOnSnapshot bool
	// EventsRetentionCount is the number of events to retain before the
	// snapshot point. A value of 0 means delete all events up to the snapshot.
	// A value of N means keep the last N events before the snapshot as a safety margin.
	EventsRetentionCount uint64
}
