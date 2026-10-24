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

package testkit

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/encryption"
	testpb "github.com/tochemey/ego/v5/test/data/testpb"
)

// ---------------------------------------------------------------------------
// EventStore tests
// ---------------------------------------------------------------------------

func TestEventStore_NewEventsStore(t *testing.T) {
	store := NewEventsStore()
	require.NotNil(t, store)
}

func TestEventStore_Connect(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	t.Run("fresh connect", func(t *testing.T) {
		err := store.Connect(ctx)
		require.NoError(t, err)
	})
	t.Run("already connected", func(t *testing.T) {
		err := store.Connect(ctx)
		require.NoError(t, err)
	})
}

func TestEventStore_Disconnect(t *testing.T) {
	ctx := context.TODO()
	t.Run("connected store", func(t *testing.T) {
		store := NewEventsStore()
		require.NoError(t, store.Connect(ctx))
		err := store.Disconnect(ctx)
		require.NoError(t, err)
	})
	t.Run("already disconnected", func(t *testing.T) {
		store := NewEventsStore()
		err := store.Disconnect(ctx)
		require.NoError(t, err)
	})
	t.Run("forgets the events", func(t *testing.T) {
		store := NewEventsStore()
		require.NoError(t, store.Connect(ctx))
		anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
			{PersistenceId: "disconnect-test", SequenceNumber: 1, Event: anyEvent, Timestamp: 100, Shard: 1},
		}))
		require.NoError(t, store.Disconnect(ctx))

		events, err := store.EventsByShard(ctx, 1, 0, 10)
		require.NoError(t, err)
		assert.Empty(t, events, "a disconnected store reads no event of a shard")

		offsets, err := store.ShardOffsets(ctx)
		require.NoError(t, err)
		assert.Empty(t, offsets, "a disconnected store has no shard")
	})
}

func TestEventStore_Ping(t *testing.T) {
	ctx := context.TODO()
	t.Run("when connected", func(t *testing.T) {
		store := NewEventsStore()
		require.NoError(t, store.Connect(ctx))
		err := store.Ping(ctx)
		require.NoError(t, err)
	})
	t.Run("when not connected auto-connects", func(t *testing.T) {
		store := NewEventsStore()
		err := store.Ping(ctx)
		require.NoError(t, err)
	})
}

func TestEventStore_WriteAndReplayEvents(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))

	anyEvent, err := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
	require.NoError(t, err)

	events := []*egopb.Event{
		{PersistenceId: "entity-1", SequenceNumber: 1, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
		{PersistenceId: "entity-1", SequenceNumber: 2, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
		{PersistenceId: "entity-1", SequenceNumber: 3, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
	}

	require.NoError(t, store.WriteEvents(ctx, events))

	t.Run("replay all events", func(t *testing.T) {
		replayed, err := store.ReplayEvents(ctx, "entity-1", 1, 3, 10)
		require.NoError(t, err)
		assert.Len(t, replayed, 3)
	})

	t.Run("replay with limit", func(t *testing.T) {
		replayed, err := store.ReplayEvents(ctx, "entity-1", 1, 3, 2)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(replayed), 2)
	})

	t.Run("replay non-existent entity", func(t *testing.T) {
		replayed, err := store.ReplayEvents(ctx, "non-existent", 1, 10, 100)
		require.NoError(t, err)
		assert.Empty(t, replayed)
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestEventStore_GetLatestEvent(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))

	t.Run("no events returns nil", func(t *testing.T) {
		event, err := store.GetLatestEvent(ctx, "non-existent")
		require.NoError(t, err)
		assert.Nil(t, event)
	})

	t.Run("returns latest by sequence number", func(t *testing.T) {
		anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
		events := []*egopb.Event{
			{PersistenceId: "latest-test", SequenceNumber: 1, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
			{PersistenceId: "latest-test", SequenceNumber: 5, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
			{PersistenceId: "latest-test", SequenceNumber: 3, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
		}
		require.NoError(t, store.WriteEvents(ctx, events))

		latest, err := store.GetLatestEvent(ctx, "latest-test")
		require.NoError(t, err)
		require.NotNil(t, latest)
		assert.EqualValues(t, 5, latest.GetSequenceNumber())
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestEventStore_DeleteEvents(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))

	anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
	events := []*egopb.Event{
		{PersistenceId: "del-test", SequenceNumber: 1, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
		{PersistenceId: "del-test", SequenceNumber: 2, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
		{PersistenceId: "del-test", SequenceNumber: 3, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
	}
	require.NoError(t, store.WriteEvents(ctx, events))

	err := store.DeleteEvents(ctx, "del-test", 2)
	require.NoError(t, err)

	replayed, err := store.ReplayEvents(ctx, "del-test", 1, 3, 10)
	require.NoError(t, err)
	assert.Len(t, replayed, 1)
	assert.EqualValues(t, 3, replayed[0].GetSequenceNumber())

	byShard, err := store.EventsByShard(ctx, 1, 0, 10)
	require.NoError(t, err)
	require.Len(t, byShard, 1, "a deleted event is no longer read from its shard")
	assert.EqualValues(t, 3, byShard[0].GetSequenceNumber())

	require.NoError(t, store.Disconnect(ctx))
}

func TestEventStore_PersistenceIDs(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))

	t.Run("empty store", func(t *testing.T) {
		ids, nextToken, err := store.PersistenceIDs(ctx, 10, "")
		require.NoError(t, err)
		assert.Empty(t, ids)
		assert.Empty(t, nextToken)
	})

	t.Run("with events and pagination", func(t *testing.T) {
		anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
		events := []*egopb.Event{
			{PersistenceId: "pid-a", SequenceNumber: 1, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
			{PersistenceId: "pid-b", SequenceNumber: 1, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
			{PersistenceId: "pid-c", SequenceNumber: 1, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
		}
		require.NoError(t, store.WriteEvents(ctx, events))

		ids, nextToken, err := store.PersistenceIDs(ctx, 2, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"pid-a", "pid-b"}, ids)
		assert.Equal(t, "pid-b", nextToken)

		ids2, lastToken, err := store.PersistenceIDs(ctx, 2, nextToken)
		require.NoError(t, err)
		assert.Equal(t, []string{"pid-c"}, ids2)
		assert.Empty(t, lastToken)
	})

	t.Run("walks every persistence ID exactly once one page at a time", func(t *testing.T) {
		var (
			walked    []string
			pageToken string
		)

		for {
			ids, nextToken, err := store.PersistenceIDs(ctx, 1, pageToken)
			require.NoError(t, err)
			walked = append(walked, ids...)
			if nextToken == "" {
				break
			}
			pageToken = nextToken
		}

		assert.Equal(t, []string{"pid-a", "pid-b", "pid-c"}, walked)
	})

	t.Run("returns nothing after the last persistence ID", func(t *testing.T) {
		ids, nextToken, err := store.PersistenceIDs(ctx, 10, "pid-c")
		require.NoError(t, err)
		assert.Empty(t, ids)
		assert.Empty(t, nextToken)
	})

	t.Run("accepts any page size", func(t *testing.T) {
		ids, nextToken, err := store.PersistenceIDs(ctx, math.MaxUint64, "pid-a")
		require.NoError(t, err)
		assert.Equal(t, []string{"pid-b", "pid-c"}, ids)
		assert.Empty(t, nextToken)
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestEventStore_EventsByShard(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))

	t.Run("no events for shard", func(t *testing.T) {
		events, err := store.EventsByShard(ctx, 99, 0, 10)
		require.NoError(t, err)
		assert.Empty(t, events)
	})

	t.Run("returns the events of the shard only", func(t *testing.T) {
		anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
		ts := time.Now().UnixNano()
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
			{PersistenceId: "shard-test-1", SequenceNumber: 1, Event: anyEvent, Timestamp: ts, Shard: 5},
			{PersistenceId: "shard-test-2", SequenceNumber: 1, Event: anyEvent, Timestamp: ts + 1, Shard: 5},
			{PersistenceId: "shard-test-3", SequenceNumber: 1, Event: anyEvent, Timestamp: ts + 2, Shard: 6},
		}))

		result, err := store.EventsByShard(ctx, 5, 0, 10)
		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, "shard-test-1", result[0].GetPersistenceId())
		assert.Equal(t, "shard-test-2", result[1].GetPersistenceId())
	})

	t.Run("includes the events at the given timestamp", func(t *testing.T) {
		anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-3", AccountBalance: 100})
		ts := time.Now().UnixNano()
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
			{PersistenceId: "from-test-1", SequenceNumber: 1, Event: anyEvent, Timestamp: ts, Shard: 8},
			{PersistenceId: "from-test-2", SequenceNumber: 1, Event: anyEvent, Timestamp: ts + 1, Shard: 8},
		}))

		result, err := store.EventsByShard(ctx, 8, ts+1, 10)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, "from-test-2", result[0].GetPersistenceId())
	})

	t.Run("orders by timestamp then sequence number and honors the limit", func(t *testing.T) {
		anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-2", AccountBalance: 100})
		ts := time.Now().UnixNano()
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
			{PersistenceId: "limit-test-1", SequenceNumber: 2, Event: anyEvent, Timestamp: ts, Shard: 7},
			{PersistenceId: "limit-test-2", SequenceNumber: 1, Event: anyEvent, Timestamp: ts, Shard: 7},
			{PersistenceId: "limit-test-3", SequenceNumber: 1, Event: anyEvent, Timestamp: ts + 1, Shard: 7},
		}))

		result, err := store.EventsByShard(ctx, 7, 0, 2)
		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, "limit-test-2", result[0].GetPersistenceId())
		assert.Equal(t, "limit-test-1", result[1].GetPersistenceId())
	})

	t.Run("orders events sharing a timestamp and sequence number by persistence ID", func(t *testing.T) {
		anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-4", AccountBalance: 100})
		ts := time.Now().UnixNano()
		// six persistence IDs: the store holds them in no particular order, and
		// only one of their 720 orders is already sorted
		persistenceIDs := []string{"tie-test-f", "tie-test-c", "tie-test-a", "tie-test-e", "tie-test-b", "tie-test-d"}
		for _, persistenceID := range persistenceIDs {
			require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
				{PersistenceId: persistenceID, SequenceNumber: 1, Event: anyEvent, Timestamp: ts, Shard: 9},
			}))
		}

		result, err := store.EventsByShard(ctx, 9, 0, 5)
		require.NoError(t, err)
		read := make([]string, 0, len(result))
		for _, event := range result {
			read = append(read, event.GetPersistenceId())
		}
		assert.Equal(t, []string{"tie-test-a", "tie-test-b", "tie-test-c", "tie-test-d", "tie-test-e"}, read)
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestEventStore_WriteEventsRejectsStoredEvents(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	anyEvent, err := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
	require.NoError(t, err)

	require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
		{PersistenceId: "entity-1", SequenceNumber: 1, Event: anyEvent},
	}))

	// the second event is new, but the first is stored already: nothing is written
	err = store.WriteEvents(ctx, []*egopb.Event{
		{PersistenceId: "entity-1", SequenceNumber: 2, Event: anyEvent},
		{PersistenceId: "entity-1", SequenceNumber: 1, Event: anyEvent},
	})
	require.Error(t, err)

	latest, err := store.GetLatestEvent(ctx, "entity-1")
	require.NoError(t, err)
	assert.EqualValues(t, 1, latest.GetSequenceNumber())

	// one write holding the same event twice is rejected too, and stores nothing
	err = store.WriteEvents(ctx, []*egopb.Event{
		{PersistenceId: "entity-2", SequenceNumber: 1, Event: anyEvent},
		{PersistenceId: "entity-2", SequenceNumber: 1, Event: anyEvent},
	})
	require.Error(t, err)

	latest, err = store.GetLatestEvent(ctx, "entity-2")
	require.NoError(t, err)
	assert.Nil(t, latest)
}

func TestEventStore_ShardOffsets(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))

	anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
	events := []*egopb.Event{
		{PersistenceId: "sn-1", SequenceNumber: 1, Event: anyEvent, Timestamp: 100, Shard: 1},
		{PersistenceId: "sn-2", SequenceNumber: 1, Event: anyEvent, Timestamp: 300, Shard: 2},
		{PersistenceId: "sn-3", SequenceNumber: 1, Event: anyEvent, Timestamp: 200, Shard: 1},
	}
	require.NoError(t, store.WriteEvents(ctx, events))

	offsets, err := store.ShardOffsets(ctx)
	require.NoError(t, err)
	assert.Len(t, offsets, 2)
	// each shard reports the timestamp of its most recent event
	assert.Equal(t, int64(200), offsets[1])
	assert.Equal(t, int64(300), offsets[2])

	require.NoError(t, store.Disconnect(ctx))
}

// ---------------------------------------------------------------------------
// DurableStore tests
// ---------------------------------------------------------------------------

func TestDurableStore_NewDurableStore(t *testing.T) {
	store := NewDurableStore()
	require.NotNil(t, store)
}

func TestDurableStore_Connect(t *testing.T) {
	ctx := context.TODO()
	store := NewDurableStore()
	t.Run("fresh connect", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
	})
	t.Run("already connected", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
	})
}

func TestDurableStore_Disconnect(t *testing.T) {
	ctx := context.TODO()
	t.Run("connected store", func(t *testing.T) {
		store := NewDurableStore()
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Disconnect(ctx))
	})
	t.Run("already disconnected", func(t *testing.T) {
		store := NewDurableStore()
		require.NoError(t, store.Disconnect(ctx))
	})
}

func TestDurableStore_Ping(t *testing.T) {
	ctx := context.TODO()
	t.Run("when connected", func(t *testing.T) {
		store := NewDurableStore()
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Ping(ctx))
	})
	t.Run("when not connected auto-connects", func(t *testing.T) {
		store := NewDurableStore()
		require.NoError(t, store.Ping(ctx))
	})
}

func TestDurableStore_WriteAndGetState(t *testing.T) {
	ctx := context.TODO()
	store := NewDurableStore()
	require.NoError(t, store.Connect(ctx))

	anyState, err := anypb.New(&testpb.Account{AccountId: "acc-1", AccountBalance: 500})
	require.NoError(t, err)

	state := &egopb.DurableState{
		PersistenceId:  "ds-entity-1",
		ResultingState: anyState,
		VersionNumber:  1,
	}

	t.Run("write and read state", func(t *testing.T) {
		require.NoError(t, store.WriteState(ctx, state))
		got, err := store.GetLatestState(ctx, "ds-entity-1")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, state.GetPersistenceId(), got.GetPersistenceId())
		assert.EqualValues(t, 1, got.GetVersionNumber())
	})

	t.Run("get non-existent state", func(t *testing.T) {
		got, err := store.GetLatestState(ctx, "non-existent")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestDurableStore_WriteState_NotConnected(t *testing.T) {
	ctx := context.TODO()
	store := NewDurableStore()
	anyState, _ := anypb.New(&testpb.Account{AccountId: "acc-1", AccountBalance: 100})
	state := &egopb.DurableState{PersistenceId: "entity-1", ResultingState: anyState, VersionNumber: 1}
	err := store.WriteState(ctx, state)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestDurableStore_GetLatestState_NotConnected(t *testing.T) {
	ctx := context.TODO()
	store := NewDurableStore()
	got, err := store.GetLatestState(ctx, "entity-1")
	require.Error(t, err)
	assert.Nil(t, got)
}

// ---------------------------------------------------------------------------
// OffsetStore tests
// ---------------------------------------------------------------------------

func TestOffsetStore_NewOffsetStore(t *testing.T) {
	store := NewOffsetStore()
	require.NotNil(t, store)
}

func TestOffsetStore_Connect(t *testing.T) {
	ctx := context.TODO()
	store := NewOffsetStore()
	t.Run("fresh connect", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
	})
	t.Run("already connected", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
	})
}

func TestOffsetStore_Disconnect(t *testing.T) {
	ctx := context.TODO()
	t.Run("connected store", func(t *testing.T) {
		store := NewOffsetStore()
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Disconnect(ctx))
	})
	t.Run("already disconnected", func(t *testing.T) {
		store := NewOffsetStore()
		require.NoError(t, store.Disconnect(ctx))
	})
	t.Run("forgets the offsets", func(t *testing.T) {
		store := NewOffsetStore()
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))
		require.NoError(t, store.Disconnect(ctx))

		// the same offset written after reconnecting is stored again
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))

		latest, err := store.LatestOffsets(ctx, balancesProjection)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]int64{1: 10}, latest)
		require.NoError(t, store.Disconnect(ctx))
	})
}

func TestOffsetStore_Ping(t *testing.T) {
	ctx := context.TODO()
	t.Run("when connected", func(t *testing.T) {
		store := NewOffsetStore()
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Ping(ctx))
	})
	t.Run("when not connected auto-connects", func(t *testing.T) {
		store := NewOffsetStore()
		require.NoError(t, store.Ping(ctx))
	})
}

// ---------------------------------------------------------------------------
// SnapshotStore tests
// ---------------------------------------------------------------------------

func TestSnapshotStore_NewSnapshotStore(t *testing.T) {
	store := NewSnapshotStore()
	require.NotNil(t, store)
}

func TestSnapshotStore_Connect(t *testing.T) {
	ctx := context.TODO()
	store := NewSnapshotStore()
	t.Run("fresh connect", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
	})
	t.Run("already connected", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
	})
}

func TestSnapshotStore_Disconnect(t *testing.T) {
	ctx := context.TODO()
	t.Run("connected store", func(t *testing.T) {
		store := NewSnapshotStore()
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Disconnect(ctx))
	})
	t.Run("already disconnected", func(t *testing.T) {
		store := NewSnapshotStore()
		require.NoError(t, store.Disconnect(ctx))
	})
}

func TestSnapshotStore_Ping(t *testing.T) {
	ctx := context.TODO()
	t.Run("when connected", func(t *testing.T) {
		store := NewSnapshotStore()
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Ping(ctx))
	})
	t.Run("when not connected auto-connects", func(t *testing.T) {
		store := NewSnapshotStore()
		require.NoError(t, store.Ping(ctx))
	})
}

func TestSnapshotStore_WriteAndGetSnapshot(t *testing.T) {
	ctx := context.TODO()
	store := NewSnapshotStore()
	require.NoError(t, store.Connect(ctx))

	anyState, err := anypb.New(&testpb.Account{AccountId: "acc-1", AccountBalance: 500})
	require.NoError(t, err)

	snapshots := []*egopb.Snapshot{
		{PersistenceId: "snap-entity-1", SequenceNumber: 1, State: anyState},
		{PersistenceId: "snap-entity-1", SequenceNumber: 5, State: anyState},
		{PersistenceId: "snap-entity-1", SequenceNumber: 3, State: anyState},
	}

	for _, snap := range snapshots {
		require.NoError(t, store.WriteSnapshot(ctx, snap))
	}

	t.Run("returns latest snapshot", func(t *testing.T) {
		got, err := store.GetLatestSnapshot(ctx, "snap-entity-1")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.EqualValues(t, 5, got.GetSequenceNumber())
	})

	t.Run("no snapshot returns nil", func(t *testing.T) {
		got, err := store.GetLatestSnapshot(ctx, "non-existent")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestSnapshotStore_DeleteSnapshots(t *testing.T) {
	ctx := context.TODO()
	store := NewSnapshotStore()
	require.NoError(t, store.Connect(ctx))

	anyState, _ := anypb.New(&testpb.Account{AccountId: "acc-1", AccountBalance: 100})
	snapshots := []*egopb.Snapshot{
		{PersistenceId: "del-snap", SequenceNumber: 1, State: anyState},
		{PersistenceId: "del-snap", SequenceNumber: 2, State: anyState},
		{PersistenceId: "del-snap", SequenceNumber: 3, State: anyState},
	}
	for _, snap := range snapshots {
		require.NoError(t, store.WriteSnapshot(ctx, snap))
	}

	require.NoError(t, store.DeleteSnapshots(ctx, "del-snap", 2))

	got, err := store.GetLatestSnapshot(ctx, "del-snap")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.EqualValues(t, 3, got.GetSequenceNumber())

	require.NoError(t, store.Disconnect(ctx))
}

// ---------------------------------------------------------------------------
// KeyStore tests
// ---------------------------------------------------------------------------

func TestKeyStore_NewKeyStore(t *testing.T) {
	store := NewKeyStore()
	require.NotNil(t, store)
}

func TestKeyStore_GetOrCreateKey(t *testing.T) {
	ctx := context.TODO()
	store := NewKeyStore()

	t.Run("creates new key", func(t *testing.T) {
		keyID, key, err := store.GetOrCreateKey(ctx, "entity-1")
		require.NoError(t, err)
		assert.NotEmpty(t, keyID)
		assert.Len(t, key, 32)
	})

	t.Run("returns existing key", func(t *testing.T) {
		keyID1, key1, err := store.GetOrCreateKey(ctx, "entity-2")
		require.NoError(t, err)

		keyID2, key2, err := store.GetOrCreateKey(ctx, "entity-2")
		require.NoError(t, err)

		assert.Equal(t, keyID1, keyID2)
		assert.Equal(t, key1, key2)
	})
}

func TestKeyStore_GetKey(t *testing.T) {
	ctx := context.TODO()
	store := NewKeyStore()

	t.Run("existing key", func(t *testing.T) {
		keyID, expectedKey, err := store.GetOrCreateKey(ctx, "entity-1")
		require.NoError(t, err)

		key, err := store.GetKey(ctx, keyID)
		require.NoError(t, err)
		assert.Equal(t, expectedKey, key)
	})

	t.Run("non-existent key", func(t *testing.T) {
		_, err := store.GetKey(ctx, "non-existent-key-id")
		require.Error(t, err)
		assert.ErrorIs(t, err, encryption.ErrKeyNotFound)
	})
}

func TestKeyStore_DeleteKey(t *testing.T) {
	ctx := context.TODO()
	store := NewKeyStore()

	t.Run("delete existing key", func(t *testing.T) {
		keyID, _, err := store.GetOrCreateKey(ctx, "entity-to-delete")
		require.NoError(t, err)

		err = store.DeleteKey(ctx, "entity-to-delete")
		require.NoError(t, err)

		_, err = store.GetKey(ctx, keyID)
		require.ErrorIs(t, err, encryption.ErrKeyNotFound)
	})

	t.Run("delete non-existent key is no-op", func(t *testing.T) {
		err := store.DeleteKey(ctx, "non-existent")
		require.NoError(t, err)
	})
}

func TestDurableStore_DeleteState(t *testing.T) {
	ctx := context.Background()
	store := NewDurableStore()
	require.NoError(t, store.Connect(ctx))

	persistenceID := "account-1"
	require.NoError(t, store.WriteState(ctx, &egopb.DurableState{PersistenceId: persistenceID, VersionNumber: 1, Shard: 7}))
	require.NoError(t, store.WriteState(ctx, &egopb.DurableState{PersistenceId: "account-2", VersionNumber: 1}))

	// a version keeps a tombstone: the version and shard stay, the state goes
	require.NoError(t, store.DeleteState(ctx, persistenceID, 2))

	state, err := store.GetLatestState(ctx, persistenceID)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.EqualValues(t, 2, state.GetVersionNumber())
	assert.EqualValues(t, 7, state.GetShard())
	assert.Nil(t, state.GetResultingState())

	// version zero removes the record
	require.NoError(t, store.DeleteState(ctx, persistenceID, 0))

	state, err = store.GetLatestState(ctx, persistenceID)
	require.NoError(t, err)
	assert.Nil(t, state)

	other, err := store.GetLatestState(ctx, "account-2")
	require.NoError(t, err)
	assert.EqualValues(t, 1, other.GetVersionNumber())

	// a persistence id without state is not an error either way
	require.NoError(t, store.DeleteState(ctx, "unknown", 0))
	require.NoError(t, store.DeleteState(ctx, "unknown", 1))

	require.NoError(t, store.Disconnect(ctx))
	require.Error(t, store.DeleteState(ctx, persistenceID, 0))
}

func TestSnapshotStore_PersistenceIDs(t *testing.T) {
	ctx := context.Background()
	store := NewSnapshotStore()
	require.NoError(t, store.Connect(ctx))

	persistenceIDs, nextPageToken, err := store.PersistenceIDs(ctx, 2, "")
	require.NoError(t, err)
	assert.Empty(t, persistenceIDs, "an empty store lists nothing")
	assert.Empty(t, nextPageToken)

	for _, snapshot := range []*egopb.Snapshot{
		{PersistenceId: "c", SequenceNumber: 1},
		{PersistenceId: "a", SequenceNumber: 1},
		{PersistenceId: "a", SequenceNumber: 2},
		{PersistenceId: "b", SequenceNumber: 1},
	} {
		require.NoError(t, store.WriteSnapshot(ctx, snapshot))
	}

	persistenceIDs, nextPageToken, err = store.PersistenceIDs(ctx, 2, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, persistenceIDs, "each persistence ID is listed once, sorted")
	assert.Equal(t, "b", nextPageToken, "the token is the last persistence ID of the page")

	persistenceIDs, nextPageToken, err = store.PersistenceIDs(ctx, 2, nextPageToken)
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, persistenceIDs)
	assert.Empty(t, nextPageToken, "the last page has no token")
}

func TestEventStore_ReplayEventsWithoutLimit(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))

	anyEvent, err := anypb.New(&testpb.AccountCreated{AccountId: "replay-limit"})
	require.NoError(t, err)
	require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
		{PersistenceId: "replay-limit", SequenceNumber: 1, Event: anyEvent},
		{PersistenceId: "replay-limit", SequenceNumber: 2, Event: anyEvent},
	}))

	// a limit above the largest int must not overflow into a negative one
	events, err := store.ReplayEvents(ctx, "replay-limit", 1, 2, math.MaxUint64)
	require.NoError(t, err)
	assert.Len(t, events, 2)
}
