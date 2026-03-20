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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/encryption"
	testpb "github.com/tochemey/ego/v4/test/data/testpb"
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
		assert.Len(t, ids, 2)
		assert.NotEmpty(t, nextToken)

		ids2, _, err := store.PersistenceIDs(ctx, 10, nextToken)
		require.NoError(t, err)
		assert.NotEmpty(t, ids2)
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestEventStore_GetShardEvents(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))

	t.Run("no events for shard", func(t *testing.T) {
		events, nextOffset, err := store.GetShardEvents(ctx, 99, 0, 10)
		require.NoError(t, err)
		assert.Empty(t, events)
		assert.EqualValues(t, 0, nextOffset)
	})

	t.Run("with shard events", func(t *testing.T) {
		anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
		ts := time.Now().UnixMilli()
		events := []*egopb.Event{
			{PersistenceId: "shard-test-1", SequenceNumber: 1, Event: anyEvent, Timestamp: ts, Shard: 5},
			{PersistenceId: "shard-test-2", SequenceNumber: 1, Event: anyEvent, Timestamp: ts + 1, Shard: 5},
			{PersistenceId: "shard-test-3", SequenceNumber: 1, Event: anyEvent, Timestamp: ts + 2, Shard: 6},
		}
		require.NoError(t, store.WriteEvents(ctx, events))

		result, nextOffset, err := store.GetShardEvents(ctx, 5, 0, 10)
		require.NoError(t, err)
		assert.NotEmpty(t, result)
		assert.Greater(t, nextOffset, int64(0))
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestEventStore_ShardNumbers(t *testing.T) {
	ctx := context.TODO()
	store := NewEventsStore()
	require.NoError(t, store.Connect(ctx))

	anyEvent, _ := anypb.New(&testpb.AccountCreated{AccountId: "acc-1", AccountBalance: 100})
	events := []*egopb.Event{
		{PersistenceId: "sn-1", SequenceNumber: 1, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
		{PersistenceId: "sn-2", SequenceNumber: 1, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 2},
		{PersistenceId: "sn-3", SequenceNumber: 1, Event: anyEvent, Timestamp: time.Now().UnixMilli(), Shard: 1},
	}
	require.NoError(t, store.WriteEvents(ctx, events))

	shards, err := store.ShardNumbers(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(shards), 2)

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

func TestOffsetStore_WriteAndGetOffset(t *testing.T) {
	ctx := context.TODO()
	store := NewOffsetStore()
	require.NoError(t, store.Connect(ctx))

	offset := &egopb.Offset{
		ProjectionName: "proj-1",
		ShardNumber:    1,
		Value:          100,
		Timestamp:      time.Now().UnixMilli(),
	}

	t.Run("write and read offset", func(t *testing.T) {
		require.NoError(t, store.WriteOffset(ctx, offset))

		projID := &egopb.ProjectionId{
			ProjectionName: "proj-1",
			ShardNumber:    1,
		}
		got, err := store.GetCurrentOffset(ctx, projID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.EqualValues(t, 100, got.GetValue())
	})

	t.Run("get non-existent offset", func(t *testing.T) {
		projID := &egopb.ProjectionId{
			ProjectionName: "non-existent",
			ShardNumber:    99,
		}
		got, err := store.GetCurrentOffset(ctx, projID)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	require.NoError(t, store.Disconnect(ctx))
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
