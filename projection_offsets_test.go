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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/tochemey/ego/v5/egopb"
)

func TestNewShardState(t *testing.T) {
	t.Run("starts at the floor when the shard has no offset", func(t *testing.T) {
		state := newShardState(nil, 42, time.Minute)

		assert.EqualValues(t, 42, state.latest)
		assert.Empty(t, state.entities)
	})

	t.Run("starts at the newest offset and keeps the highest sequence number of each persistence ID", func(t *testing.T) {
		window := time.Minute
		newest := time.Now().UnixNano()
		offsets := []*egopb.EntityOffset{
			{PersistenceId: "account-1", SequenceNumber: 4, Timestamp: newest},
			{PersistenceId: "account-1", SequenceNumber: 3, Timestamp: newest - 1},
			{PersistenceId: "account-2", SequenceNumber: 7, Timestamp: newest - 2},
		}

		state := newShardState(offsets, 0, window)

		assert.Equal(t, newest, state.latest)
		assert.Equal(t, map[string]entityRecord{
			"account-1": {sequenceNumber: 4, timestamp: newest},
			"account-2": {sequenceNumber: 7, timestamp: newest - 2},
		}, state.entities)
	})

	t.Run("keeps the floor when it is ahead of every offset", func(t *testing.T) {
		state := newShardState([]*egopb.EntityOffset{{PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 10}}, 100, time.Minute)

		assert.EqualValues(t, 100, state.latest)
	})

	t.Run("forgets the persistence IDs older than the time window", func(t *testing.T) {
		window := time.Minute
		newest := time.Now().UnixNano()
		offsets := []*egopb.EntityOffset{
			{PersistenceId: "recent", SequenceNumber: 1, Timestamp: newest},
			{PersistenceId: "boundary", SequenceNumber: 1, Timestamp: newest - window.Nanoseconds()},
			{PersistenceId: "stale", SequenceNumber: 1, Timestamp: newest - window.Nanoseconds() - 1},
		}

		state := newShardState(offsets, 0, window)

		assert.Contains(t, state.entities, "recent")
		assert.Contains(t, state.entities, "boundary", "a persistence ID exactly a window behind is kept")
		assert.NotContains(t, state.entities, "stale")
	})
}

func TestShardStateRemember(t *testing.T) {
	state := newShardState(nil, 0, time.Minute)

	state.remember("account-1", 2, 20)
	assert.Equal(t, entityRecord{sequenceNumber: 2, timestamp: 20}, state.entities["account-1"])
	assert.EqualValues(t, 20, state.latest)

	// a lower sequence number never replaces a higher one
	state.remember("account-1", 1, 30)
	assert.Equal(t, entityRecord{sequenceNumber: 2, timestamp: 20}, state.entities["account-1"])
	assert.EqualValues(t, 20, state.latest)

	// an older event of another persistence ID does not move the position back
	state.remember("account-2", 1, 10)
	assert.EqualValues(t, 20, state.latest)
}

func TestEntityRecordCheck(t *testing.T) {
	record := entityRecord{sequenceNumber: 5}

	assert.Equal(t, duplicate, record.check(4))
	assert.Equal(t, duplicate, record.check(5))
	assert.Equal(t, accepted, record.check(6))
	assert.Equal(t, rejected, record.check(7))
}

func TestShardStateShouldBacktrack(t *testing.T) {
	settings := defaultOffsetSettings()
	now := time.Now()
	recent := now.Add(-time.Second).UnixNano()
	const bufferSize = 10

	t.Run("not when already backtracking", func(t *testing.T) {
		state := &shardState{latest: recent, backtracking: true, idlePasses: backtrackingIdlePasses}
		assert.False(t, state.shouldBacktrack(settings, bufferSize, now))
	})

	t.Run("not before the shard has a position", func(t *testing.T) {
		state := &shardState{idlePasses: backtrackingIdlePasses}
		assert.False(t, state.shouldBacktrack(settings, bufferSize, now))
	})

	t.Run("not when the position is older than the backtracking reach", func(t *testing.T) {
		reach := settings.backtrackingWindow + settings.backtrackingBehindCurrentTime
		state := &shardState{latest: now.Add(-reach - time.Second).UnixNano(), idlePasses: backtrackingIdlePasses}
		assert.False(t, state.shouldBacktrack(settings, bufferSize, now))
	})

	t.Run("when the shard has been idle for a few passes", func(t *testing.T) {
		state := &shardState{latest: recent, latestBacktracking: recent, idlePasses: backtrackingIdlePasses}
		assert.True(t, state.shouldBacktrack(settings, bufferSize, now))
	})

	t.Run("when several buffers were read since the last backtracking", func(t *testing.T) {
		state := &shardState{latest: recent, latestBacktracking: recent, readSinceBacktracking: backtrackingBufferMultiple * bufferSize}
		assert.True(t, state.shouldBacktrack(settings, bufferSize, now))
	})

	t.Run("when the position is more than half a window past the last backtracking", func(t *testing.T) {
		state := &shardState{latest: recent, latestBacktracking: recent - (settings.backtrackingWindow / 2).Nanoseconds() - 1}
		assert.True(t, state.shouldBacktrack(settings, bufferSize, now))
	})

	t.Run("not when none of the triggers holds", func(t *testing.T) {
		state := &shardState{latest: recent, latestBacktracking: recent}
		assert.False(t, state.shouldBacktrack(settings, bufferSize, now))
	})
}

func TestShardStateBacktrackingRange(t *testing.T) {
	settings := defaultOffsetSettings()
	now := time.Now()
	window := settings.backtrackingWindow.Nanoseconds()
	behind := settings.backtrackingBehindCurrentTime.Nanoseconds()

	t.Run("the first backtracking reaches the whole window behind the position", func(t *testing.T) {
		latest := now.Add(-time.Minute).UnixNano()
		state := &shardState{latest: latest}

		from, to := state.backtrackingRange(settings, now)
		assert.Equal(t, latest-window-behind, from)
		assert.Equal(t, latest, to)
	})

	t.Run("later ones continue where the previous one stopped", func(t *testing.T) {
		latest := now.Add(-time.Minute).UnixNano()
		state := &shardState{latest: latest, latestBacktracking: latest - time.Second.Nanoseconds()}

		from, _ := state.backtrackingRange(settings, now)
		assert.Equal(t, latest-time.Second.Nanoseconds(), from)
	})

	t.Run("never further back than the window", func(t *testing.T) {
		latest := now.Add(-time.Minute).UnixNano()
		state := &shardState{latest: latest, latestBacktracking: latest - 2*window}

		from, _ := state.backtrackingRange(settings, now)
		assert.Equal(t, latest-window, from)
	})

	t.Run("stops short of the events younger than behind-current-time", func(t *testing.T) {
		state := &shardState{latest: now.UnixNano()}

		_, to := state.backtrackingRange(settings, now)
		assert.Equal(t, now.UnixNano()-behind, to)
	})
}

func TestShardStateStopBacktracking(t *testing.T) {
	state := &shardState{backtracking: true, idlePasses: 3, readSinceBacktracking: 40, latestBacktracking: 7}

	state.stopBacktracking()

	assert.False(t, state.backtracking)
	assert.Zero(t, state.idlePasses)
	assert.Zero(t, state.readSinceBacktracking)
	assert.EqualValues(t, 7, state.latestBacktracking, "the next backtracking resumes from where this one stopped")
}
