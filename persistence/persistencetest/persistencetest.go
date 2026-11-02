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

// Package persistencetest checks that implementations of the persistence
// interfaces honor the contracts their godoc states.
//
// Call TestEventsStore, TestSnapshotStore or TestStateStore from a test of the
// implementation, with a function that returns a new store on empty storage:
//
//	func TestEventsStoreContract(t *testing.T) {
//	    persistencetest.TestEventsStore(t, func(t *testing.T) persistence.EventsStore {
//	        return mystore.NewEventsStore(newEmptyDatabase(t))
//	    })
//	}
//
// Every subtest calls that function once, connects the store and disconnects
// it when the subtest ends.
package persistencetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

// maxPages bounds how many pages TestEventsStore reads while paging through
// the persistence IDs, so a store that never ends its pages fails the test
// instead of looping forever.
const maxPages = 100

// TestEventsStore runs the persistence.EventsStore contract against the stores
// newStore returns. newStore must return a store that is not connected yet, on
// storage that holds no event.
func TestEventsStore(t *testing.T, newStore func(t *testing.T) persistence.EventsStore) {
	t.Run("pings once connected", func(t *testing.T) {
		store := connectedEventsStore(t, newStore)
		require.NoError(t, store.Ping(context.Background()))
	})

	t.Run("has no latest event for an unknown persistence ID", func(t *testing.T) {
		store := connectedEventsStore(t, newStore)

		event, err := store.GetLatestEvent(context.Background(), "unknown")
		require.NoError(t, err)
		assert.Nil(t, event)
	})

	t.Run("replays the events it wrote, every field included", func(t *testing.T) {
		ctx := context.Background()
		store := connectedEventsStore(t, newStore)

		first := newEvent(t, "account-1", 1, 100, 1)
		second := newEvent(t, "account-1", 2, 200, 1)
		second.IsDeleted = true
		second.IsEncrypted = true
		second.EncryptionKeyId = "key-1"
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{first, second}))

		events, err := store.ReplayEvents(ctx, "account-1", 1, 2, 10)
		require.NoError(t, err)
		assertEvents(t, []*egopb.Event{first, second}, events)

		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assertEvents(t, []*egopb.Event{second}, []*egopb.Event{latest})
	})

	t.Run("replays an inclusive range of sequence numbers up to the limit", func(t *testing.T) {
		ctx := context.Background()
		store := connectedEventsStore(t, newStore)

		journal := make([]*egopb.Event, 0, 5)
		for sequenceNumber := uint64(1); sequenceNumber <= 5; sequenceNumber++ {
			journal = append(journal, newEvent(t, "account-1", sequenceNumber, int64(sequenceNumber)*100, 1))
		}

		require.NoError(t, store.WriteEvents(ctx, journal))

		events, err := store.ReplayEvents(ctx, "account-1", 2, 4, 10)
		require.NoError(t, err)
		assertEvents(t, journal[1:4], events)

		events, err = store.ReplayEvents(ctx, "account-1", 1, 5, 2)
		require.NoError(t, err)
		assertEvents(t, journal[:2], events)
	})

	t.Run("rejects a write holding a stored event and stores none of it", func(t *testing.T) {
		ctx := context.Background()
		store := connectedEventsStore(t, newStore)

		first := newEvent(t, "account-1", 1, 100, 1)
		second := newEvent(t, "account-1", 2, 200, 1)
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{first, second}))

		third := newEvent(t, "account-1", 3, 300, 1)
		rewritten := newEvent(t, "account-1", 2, 250, 1)
		require.Error(t, store.WriteEvents(ctx, []*egopb.Event{third, rewritten}))

		events, err := store.ReplayEvents(ctx, "account-1", 1, 3, 10)
		require.NoError(t, err)
		assertEvents(t, []*egopb.Event{first, second}, events)
	})

	t.Run("deletes the events up to a sequence number, inclusive", func(t *testing.T) {
		ctx := context.Background()
		store := connectedEventsStore(t, newStore)

		journal := []*egopb.Event{
			newEvent(t, "account-1", 1, 100, 1),
			newEvent(t, "account-1", 2, 200, 1),
			newEvent(t, "account-1", 3, 300, 1),
		}
		require.NoError(t, store.WriteEvents(ctx, journal))

		require.NoError(t, store.DeleteEvents(ctx, "account-1", 2))

		events, err := store.ReplayEvents(ctx, "account-1", 1, 3, 10)
		require.NoError(t, err)
		assertEvents(t, journal[2:], events)
	})

	t.Run("pages through every persistence ID exactly once", func(t *testing.T) {
		ctx := context.Background()
		store := connectedEventsStore(t, newStore)

		persistenceIDs := []string{"account-1", "account-2", "account-3", "account-4", "account-5"}
		for _, persistenceID := range persistenceIDs {
			require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
				newEvent(t, persistenceID, 1, 100, 1),
				newEvent(t, persistenceID, 2, 200, 1),
			}))
		}

		var (
			listed    []string
			pageToken string
		)

		for range maxPages {
			page, nextPageToken, err := store.PersistenceIDs(ctx, 2, pageToken)
			require.NoError(t, err)
			listed = append(listed, page...)

			if nextPageToken == "" {
				break
			}

			pageToken = nextPageToken
		}

		assert.ElementsMatch(t, persistenceIDs, listed)
	})

	t.Run("reads a shard from a timestamp on, in order, up to the limit", func(t *testing.T) {
		ctx := context.Background()
		store := connectedEventsStore(t, newStore)

		before := newEvent(t, "account-a", 1, 100, 7)
		atBoundSecond := newEvent(t, "account-a", 2, 200, 7)
		atBoundFirst := newEvent(t, "account-b", 1, 200, 7)
		atBoundTie := newEvent(t, "account-c", 1, 200, 7)
		after := newEvent(t, "account-a", 3, 300, 7)
		otherShard := newEvent(t, "account-d", 1, 250, 8)

		for _, event := range []*egopb.Event{before, atBoundSecond, atBoundFirst, atBoundTie, after, otherShard} {
			require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{event}))
		}

		// ordered by timestamp, then sequence number, then persistence ID
		events, err := store.EventsByShard(ctx, 7, 200, 10)
		require.NoError(t, err)
		assertEvents(t, []*egopb.Event{atBoundFirst, atBoundTie, atBoundSecond, after}, events)

		events, err = store.EventsByShard(ctx, 7, 200, 2)
		require.NoError(t, err)
		assertEvents(t, []*egopb.Event{atBoundFirst, atBoundTie}, events)
	})

	t.Run("maps every shard to the timestamp of its newest event", func(t *testing.T) {
		ctx := context.Background()
		store := connectedEventsStore(t, newStore)

		offsets, err := store.ShardOffsets(ctx)
		require.NoError(t, err)
		assert.Empty(t, offsets)

		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
			newEvent(t, "account-1", 1, 100, 3),
			newEvent(t, "account-1", 2, 300, 3),
		}))
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-2", 1, 200, 9)}))

		offsets, err = store.ShardOffsets(ctx)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]int64{3: 300, 9: 200}, offsets)
	})
}

// TestSnapshotStore runs the persistence.SnapshotStore contract against the
// stores newStore returns. newStore must return a store that is not connected
// yet, on storage that holds no snapshot.
func TestSnapshotStore(t *testing.T, newStore func(t *testing.T) persistence.SnapshotStore) {
	t.Run("pings once connected", func(t *testing.T) {
		store := connectedSnapshotStore(t, newStore)
		require.NoError(t, store.Ping(context.Background()))
	})

	t.Run("has no snapshot for an unknown persistence ID", func(t *testing.T) {
		store := connectedSnapshotStore(t, newStore)

		snapshot, err := store.GetLatestSnapshot(context.Background(), "unknown")
		require.NoError(t, err)
		assert.Nil(t, snapshot)
	})

	t.Run("returns the latest snapshot it wrote, every field included", func(t *testing.T) {
		ctx := context.Background()
		store := connectedSnapshotStore(t, newStore)

		first := newSnapshot(t, "account-1", 1, "opened")
		second := newSnapshot(t, "account-1", 2, "credited")
		second.IsEncrypted = true
		second.EncryptionKeyId = "key-1"
		require.NoError(t, store.WriteSnapshot(ctx, first))
		require.NoError(t, store.WriteSnapshot(ctx, second))

		snapshot, err := store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assertMessage(t, second, snapshot)

		other, err := store.GetLatestSnapshot(ctx, "account-2")
		require.NoError(t, err)
		assert.Nil(t, other)
	})

	t.Run("deletes the snapshots up to a sequence number, inclusive", func(t *testing.T) {
		ctx := context.Background()
		store := connectedSnapshotStore(t, newStore)

		third := newSnapshot(t, "account-1", 3, "debited")
		for _, snapshot := range []*egopb.Snapshot{
			newSnapshot(t, "account-1", 1, "opened"),
			newSnapshot(t, "account-1", 2, "credited"),
			third,
		} {
			require.NoError(t, store.WriteSnapshot(ctx, snapshot))
		}

		require.NoError(t, store.DeleteSnapshots(ctx, "account-1", 2))

		snapshot, err := store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assertMessage(t, third, snapshot)

		require.NoError(t, store.DeleteSnapshots(ctx, "account-1", 3))

		snapshot, err = store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, snapshot)
	})
}

// TestStateStore runs the persistence.StateStore contract against the stores
// newStore returns. newStore must return a store that is not connected yet, on
// storage that holds no state.
func TestStateStore(t *testing.T, newStore func(t *testing.T) persistence.StateStore) {
	t.Run("pings once connected", func(t *testing.T) {
		store := connectedStateStore(t, newStore)
		require.NoError(t, store.Ping(context.Background()))
	})

	t.Run("has no state for an unknown persistence ID", func(t *testing.T) {
		store := connectedStateStore(t, newStore)

		state, err := store.GetLatestState(context.Background(), "unknown")
		require.NoError(t, err)
		assert.Nil(t, state)
	})

	t.Run("replaces the state on every write", func(t *testing.T) {
		ctx := context.Background()
		store := connectedStateStore(t, newStore)

		latest := newDurableState(t, "account-1", 2, "credited")
		require.NoError(t, store.WriteState(ctx, newDurableState(t, "account-1", 1, "opened")))
		require.NoError(t, store.WriteState(ctx, latest))

		state, err := store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		assertMessage(t, latest, state)
	})

	t.Run("keeps a tombstone with the version when deleting with a version", func(t *testing.T) {
		ctx := context.Background()
		store := connectedStateStore(t, newStore)
		require.NoError(t, store.WriteState(ctx, newDurableState(t, "account-1", 3, "credited")))

		require.NoError(t, store.DeleteState(ctx, "account-1", 4))

		state, err := store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, uint64(4), state.GetVersionNumber())
		assert.Nil(t, state.GetResultingState())

		// a state written after the deletion replaces the tombstone
		recreated := newDurableState(t, "account-1", 5, "reopened")
		require.NoError(t, store.WriteState(ctx, recreated))

		state, err = store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		assertMessage(t, recreated, state)
	})

	t.Run("removes the state when deleting with version zero", func(t *testing.T) {
		ctx := context.Background()
		store := connectedStateStore(t, newStore)
		require.NoError(t, store.WriteState(ctx, newDurableState(t, "account-1", 1, "opened")))

		require.NoError(t, store.DeleteState(ctx, "account-1", 0))

		state, err := store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, state)
	})

	t.Run("deleting the state of an unknown persistence ID is not an error", func(t *testing.T) {
		ctx := context.Background()
		store := connectedStateStore(t, newStore)

		require.NoError(t, store.DeleteState(ctx, "unknown-1", 0))
		require.NoError(t, store.DeleteState(ctx, "unknown-2", 1))
	})
}

// connectedEventsStore creates an events store with newStore, connects it and
// disconnects it when the test ends.
func connectedEventsStore(t *testing.T, newStore func(t *testing.T) persistence.EventsStore) persistence.EventsStore {
	t.Helper()

	store := newStore(t)
	connect(t, store.Connect, store.Disconnect)
	return store
}

// connectedSnapshotStore creates a snapshot store with newStore, connects it
// and disconnects it when the test ends.
func connectedSnapshotStore(t *testing.T, newStore func(t *testing.T) persistence.SnapshotStore) persistence.SnapshotStore {
	t.Helper()

	store := newStore(t)
	connect(t, store.Connect, store.Disconnect)
	return store
}

// connectedStateStore creates a state store with newStore, connects it and
// disconnects it when the test ends.
func connectedStateStore(t *testing.T, newStore func(t *testing.T) persistence.StateStore) persistence.StateStore {
	t.Helper()

	store := newStore(t)
	connect(t, store.Connect, store.Disconnect)
	return store
}

// connect connects a store and disconnects it when the test ends.
func connect(t *testing.T, connectStore, disconnectStore func(context.Context) error) {
	t.Helper()

	require.NoError(t, connectStore(context.Background()))
	t.Cleanup(func() {
		assert.NoError(t, disconnectStore(context.Background()))
	})
}

// newEvent builds an event of the persistence ID whose payload is its sequence
// number.
func newEvent(t *testing.T, persistenceID string, sequenceNumber uint64, timestamp int64, shard uint64) *egopb.Event {
	t.Helper()

	return &egopb.Event{
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Event:          newPayload(t, wrapperspb.UInt64(sequenceNumber)),
		Timestamp:      timestamp,
		Shard:          shard,
	}
}

// newSnapshot builds a snapshot of the persistence ID whose state is the given value.
func newSnapshot(t *testing.T, persistenceID string, sequenceNumber uint64, value string) *egopb.Snapshot {
	t.Helper()

	return &egopb.Snapshot{
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		State:          newPayload(t, wrapperspb.String(value)),
		Timestamp:      int64(sequenceNumber) * 100,
	}
}

// newDurableState builds a durable state of the persistence ID whose resulting
// state is the given value.
func newDurableState(t *testing.T, persistenceID string, version uint64, value string) *egopb.DurableState {
	t.Helper()

	return &egopb.DurableState{
		PersistenceId:  persistenceID,
		VersionNumber:  version,
		ResultingState: newPayload(t, wrapperspb.String(value)),
		Timestamp:      int64(version) * 100,
		Shard:          persistence.ShardFor(persistenceID),
	}
}

// newPayload packs message into an Any.
func newPayload(t *testing.T, message proto.Message) *anypb.Any {
	t.Helper()

	payload, err := anypb.New(message)
	require.NoError(t, err)
	return payload
}

// assertEvents asserts the events equal the expected ones, in order.
func assertEvents(t *testing.T, expected, actual []*egopb.Event) {
	t.Helper()

	require.Len(t, actual, len(expected))
	for index := range expected {
		assertMessage(t, expected[index], actual[index])
	}
}

// assertMessage asserts actual equals expected.
func assertMessage(t *testing.T, expected, actual proto.Message) {
	t.Helper()

	assert.True(t, proto.Equal(expected, actual), "expected %v, got %v", expected, actual)
}
