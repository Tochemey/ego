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

import (
	"time"

	"github.com/tochemey/ego/v5/egopb"
)

const (
	// offsetTimeWindow is how far behind its newest offset on a shard a
	// projection keeps the offsets of persistence IDs in memory. It is at least
	// backtrackingWindow, so every event a backtracking read can return is
	// checked against what the projection already processed.
	offsetTimeWindow = 5 * time.Minute
	// offsetReadLimit is how many offsets of a shard a projection loads when it
	// starts reading the shard. Other persistence IDs are loaded when their
	// next event is read.
	offsetReadLimit = 100
	// offsetDeleteAfter is how far behind its newest offset on a shard an offset
	// must be before it can be deleted from the offset store.
	offsetDeleteAfter = 24 * time.Hour
	// offsetDeleteAfterConsumed is how long ago an offset must have been
	// recorded before it can be deleted from the offset store, so the offsets of
	// a projection that is rebuilt from old events are kept while it catches up.
	offsetDeleteAfterConsumed = 9 * 24 * time.Hour
	// offsetDeleteInterval is how often a projection deletes old offsets.
	offsetDeleteInterval = time.Hour
	// backtrackingWindow is how far behind its position on a shard a
	// projection reads again to find events that became visible late.
	backtrackingWindow = 2 * time.Minute
	// backtrackingBehindCurrentTime is how old an event must be before a
	// backtracking read returns it.
	backtrackingBehindCurrentTime = 10 * time.Second
	// backtrackingIdlePasses is how many consecutive passes without new events
	// on a shard trigger a backtracking read of the shard.
	backtrackingIdlePasses = 5
	// backtrackingBufferMultiple is how many buffers of events read on a shard
	// since its last backtracking read trigger the next one.
	backtrackingBufferMultiple = 3
)

// validation is the outcome of checking an event against the offsets of its
// persistence ID.
type validation int

const (
	// accepted marks an event that follows the last processed event of its
	// persistence ID, or the first event of a persistence ID that may be processed.
	accepted validation = iota
	// duplicate marks an event the projection has already processed.
	duplicate
	// rejected marks an event that comes after a gap: an earlier event of its
	// persistence ID has not been processed yet.
	rejected
)

// entityRecord is the last event of a persistence ID a projection processed.
type entityRecord struct {
	// sequenceNumber is the sequence number of the event.
	sequenceNumber uint64
	// timestamp is the timestamp of the event, in Unix nanoseconds.
	timestamp int64
}

// offsetSettings holds the windows and intervals that govern how a projection
// tracks and cleans its offsets. Every runner uses the defaults; tests shorten
// them.
type offsetSettings struct {
	// timeWindow is how long the offsets of a shard's persistence IDs stay in
	// memory behind its position (offsetTimeWindow by default).
	timeWindow time.Duration
	// deleteAfter is how far behind a shard's position an offset must be
	// before it is deleted (offsetDeleteAfter by default).
	deleteAfter time.Duration
	// deleteAfterConsumed is how long ago an offset must have been recorded
	// before it is deleted (offsetDeleteAfterConsumed by default).
	deleteAfterConsumed time.Duration
	// deleteInterval is how often old offsets are deleted (offsetDeleteInterval
	// by default).
	deleteInterval time.Duration
	// backtrackingWindow is how far behind a shard's position a backtracking
	// read reaches (backtrackingWindow by default).
	backtrackingWindow time.Duration
	// backtrackingBehindCurrentTime is how old an event must be before a
	// backtracking read returns it (backtrackingBehindCurrentTime by default).
	backtrackingBehindCurrentTime time.Duration
}

// defaultOffsetSettings returns the settings every runner uses.
func defaultOffsetSettings() offsetSettings {
	return offsetSettings{
		timeWindow:                    offsetTimeWindow,
		deleteAfter:                   offsetDeleteAfter,
		deleteAfterConsumed:           offsetDeleteAfterConsumed,
		deleteInterval:                offsetDeleteInterval,
		backtrackingWindow:            backtrackingWindow,
		backtrackingBehindCurrentTime: backtrackingBehindCurrentTime,
	}
}

// shardState is what a projection knows about one shard: its position, the
// persistence IDs it processed recently and its backtracking progress.
//
// A shard is processed by one worker at a time, and passes do not overlap, so
// the state needs no lock of its own.
type shardState struct {
	// latest is the newest timestamp the projection has processed on the
	// shard: the shard's position. The next normal read starts there.
	latest int64
	// entities holds the last processed event of every persistence ID of the
	// shard processed within the time window.
	entities map[string]entityRecord
	// lastReadFull reports that the last normal read of the shard returned a
	// full buffer, so more events may share the position's timestamp.
	lastReadFull bool
	// backtracking reports that the shard is read again behind its position.
	backtracking bool
	// latestBacktracking is the newest timestamp a backtracking read of the
	// shard reached, or zero before the first one.
	latestBacktracking int64
	// idlePasses counts the consecutive passes in which the shard had no new event.
	idlePasses int
	// readSinceBacktracking counts the events read on the shard since its last
	// backtracking read.
	readSinceBacktracking int
	// dirty reports that offsets were written on the shard since its old
	// offsets were last deleted.
	dirty bool
}

// newShardState builds the state of a shard from the offsets loaded for it,
// newest first, and the timestamp the projection starts from. Only the highest
// sequence number of each persistence ID is kept, and only within the time
// window behind the newest offset.
func newShardState(offsets []*egopb.EntityOffset, floor int64, timeWindow time.Duration) *shardState {
	state := &shardState{
		latest:   floor,
		entities: make(map[string]entityRecord, len(offsets)),
	}

	for _, offset := range offsets {
		state.remember(offset.GetPersistenceId(), offset.GetSequenceNumber(), offset.GetTimestamp())
	}

	state.evict(timeWindow)
	return state
}

// remember records an event of the persistence ID as processed. A record with
// a lower sequence number than the one already kept is ignored.
func (state *shardState) remember(persistenceID string, sequenceNumber uint64, timestamp int64) {
	if current, ok := state.entities[persistenceID]; ok && current.sequenceNumber >= sequenceNumber {
		return
	}

	state.entities[persistenceID] = entityRecord{sequenceNumber: sequenceNumber, timestamp: timestamp}
	state.latest = max(state.latest, timestamp)
}

// evict forgets the persistence IDs whose last processed event is older than
// the time window behind the shard's position. The persistence IDs at the
// position itself are always kept: a normal read starts at that timestamp.
func (state *shardState) evict(timeWindow time.Duration) {
	cutoff := state.latest - timeWindow.Nanoseconds()
	for persistenceID, record := range state.entities {
		if record.timestamp < cutoff {
			delete(state.entities, persistenceID)
		}
	}
}

// check validates an event against the last processed event of its
// persistence ID, when the projection knows one.
func (record entityRecord) check(sequenceNumber uint64) validation {
	switch {
	case sequenceNumber <= record.sequenceNumber:
		return duplicate
	case sequenceNumber == record.sequenceNumber+1:
		return accepted
	default:
		return rejected
	}
}

// shouldBacktrack reports whether the shard is due for a backtracking read.
//
// A shard backtracks once it has been idle for a few passes, once several
// buffers of events were read since its last backtracking read, or once its
// position is more than half a window ahead of its last backtracking read. A
// shard whose position is older than the whole backtracking window is not
// backtracked: no late event behind it is within reach anymore. Nor is a shard
// whose last backtracking read reached its position: everything behind the
// position was read again, and the normal read covers the position itself, so
// another backtracking read could find nothing until the position moves.
func (state *shardState) shouldBacktrack(settings offsetSettings, bufferSize int, now time.Time) bool {
	if state.backtracking || state.latest == 0 || state.latestBacktracking >= state.latest {
		return false
	}

	reach := settings.backtrackingWindow + settings.backtrackingBehindCurrentTime
	if state.latest < now.Add(-reach).UnixNano() {
		return false
	}

	return state.idlePasses >= backtrackingIdlePasses ||
		state.readSinceBacktracking >= backtrackingBufferMultiple*bufferSize ||
		state.latest-state.latestBacktracking > (settings.backtrackingWindow/2).Nanoseconds()
}

// backtrackingRange returns the timestamps a backtracking read of the shard
// covers, both inclusive. The first backtracking read reaches the whole window
// behind the position; later ones continue where the previous one stopped,
// never further back than the window. Events younger than
// backtrackingBehindCurrentTime are left to later reads.
func (state *shardState) backtrackingRange(settings offsetSettings, now time.Time) (from, to int64) {
	window := settings.backtrackingWindow.Nanoseconds()
	if state.latestBacktracking == 0 {
		from = state.latest - window - settings.backtrackingBehindCurrentTime.Nanoseconds()
	} else {
		from = max(state.latestBacktracking, state.latest-window)
	}

	to = min(state.latest, now.Add(-settings.backtrackingBehindCurrentTime).UnixNano())
	return from, to
}

// stopBacktracking switches the shard back to normal reads.
func (state *shardState) stopBacktracking() {
	state.backtracking = false
	state.idlePasses = 0
	state.readSinceBacktracking = 0
}
