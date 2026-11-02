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

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
)

const (
	testProjection  = "balances"
	otherProjection = "audit"
)

func TestOffsetStore(t *testing.T) {
	ctx := context.Background()
	store := NewOffsetStore(testPool)

	t.Run("connects, pings and disconnects", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Ping(ctx))
		require.NoError(t, store.Disconnect(ctx))
		require.NoError(t, testPool.Ping(ctx), "the store must leave the pool open")
	})

	t.Run("reads the offsets it wrote, newest first", func(t *testing.T) {
		resetTables(t)
		offsets := []*egopb.EntityOffset{
			{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 100, ConsumedAt: 1000},
			{ShardNumber: 1, PersistenceId: "account-2", SequenceNumber: 1, Timestamp: 300, ConsumedAt: 1001},
			{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 2, Timestamp: 200, ConsumedAt: 1002},
			{ShardNumber: 2, PersistenceId: "account-3", SequenceNumber: 1, Timestamp: 400, ConsumedAt: 1003},
		}
		require.NoError(t, store.WriteOffsets(ctx, testProjection, offsets))
		require.NoError(t, store.WriteOffsets(ctx, testProjection, offsets[:1]), "writing a stored offset is not an error")
		require.NoError(t, store.WriteOffsets(ctx, testProjection, nil))

		read, err := store.ReadOffsets(ctx, testProjection, 1, 10)
		require.NoError(t, err)
		require.Len(t, read, 3)
		assert.True(t, proto.Equal(offsets[1], read[0]))
		assert.True(t, proto.Equal(offsets[2], read[1]))
		assert.True(t, proto.Equal(offsets[0], read[2]))

		limited, err := store.ReadOffsets(ctx, testProjection, 1, 1)
		require.NoError(t, err)
		require.Len(t, limited, 1)

		other, err := store.ReadOffsets(ctx, otherProjection, 1, 10)
		require.NoError(t, err)
		assert.Empty(t, other)
	})

	t.Run("reads the offset of an entity", func(t *testing.T) {
		resetTables(t)
		latest := &egopb.EntityOffset{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 2, Timestamp: 200, ConsumedAt: 1001}
		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{
			{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 100, ConsumedAt: 1000},
			latest,
		}))

		read, err := store.ReadEntityOffset(ctx, testProjection, 1, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(latest, read))

		missing, err := store.ReadEntityOffset(ctx, testProjection, 1, "account-2")
		require.NoError(t, err)
		assert.Nil(t, missing)
	})

	t.Run("maps every shard to its newest offset", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{
			{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 100},
			{ShardNumber: 1, PersistenceId: "account-2", SequenceNumber: 1, Timestamp: 300},
			{ShardNumber: 2, PersistenceId: "account-3", SequenceNumber: 1, Timestamp: 200},
		}))

		latest, err := store.LatestOffsets(ctx, testProjection)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]int64{1: 300, 2: 200}, latest)

		none, err := store.LatestOffsets(ctx, otherProjection)
		require.NoError(t, err)
		assert.Empty(t, none)
	})

	t.Run("deletes the offsets before a timestamp and a consumption time", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{
			{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 100, ConsumedAt: 1000},
			{ShardNumber: 1, PersistenceId: "account-2", SequenceNumber: 1, Timestamp: 150, ConsumedAt: 3000},
			{ShardNumber: 1, PersistenceId: "account-3", SequenceNumber: 1, Timestamp: 300, ConsumedAt: 1000},
		}))

		require.NoError(t, store.DeleteOffsetsBefore(ctx, testProjection, 1, 200, 2000))

		remaining, err := store.ReadOffsets(ctx, testProjection, 1, 10)
		require.NoError(t, err)
		require.Len(t, remaining, 2)
		assert.Equal(t, "account-3", remaining[0].GetPersistenceId())
		assert.Equal(t, "account-2", remaining[1].GetPersistenceId())
	})

	t.Run("keeps the offsets at either bound", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{
			{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 200, ConsumedAt: 1000},
			{ShardNumber: 1, PersistenceId: "account-2", SequenceNumber: 1, Timestamp: 100, ConsumedAt: 2000},
		}))

		require.NoError(t, store.DeleteOffsetsBefore(ctx, testProjection, 1, 200, 2000))

		remaining, err := store.ReadOffsets(ctx, testProjection, 1, 10)
		require.NoError(t, err)
		assert.Len(t, remaining, 2)
	})

	t.Run("resets a projection to a start offset", func(t *testing.T) {
		resetTables(t)
		start, err := store.StartOffset(ctx, testProjection)
		require.NoError(t, err)
		assert.Zero(t, start)

		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 100}}))
		require.NoError(t, store.WriteOffsets(ctx, otherProjection, []*egopb.EntityOffset{{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 100}}))
		require.NoError(t, store.ResetOffset(ctx, testProjection, 50))
		require.NoError(t, store.ResetOffset(ctx, testProjection, 75))

		start, err = store.StartOffset(ctx, testProjection)
		require.NoError(t, err)
		assert.EqualValues(t, 75, start)

		reset, err := store.ReadOffsets(ctx, testProjection, 1, 10)
		require.NoError(t, err)
		assert.Empty(t, reset)

		untouched, err := store.ReadOffsets(ctx, otherProjection, 1, 10)
		require.NoError(t, err)
		assert.Len(t, untouched, 1)
	})

	t.Run("deletes every offset of a projection", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 100}}))
		require.NoError(t, store.ResetOffset(ctx, testProjection, 50))
		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 2, Timestamp: 200}}))

		require.NoError(t, store.DeleteOffset(ctx, testProjection))
		require.NoError(t, store.DeleteOffset(ctx, testProjection), "deleting no offset is not an error")

		start, err := store.StartOffset(ctx, testProjection)
		require.NoError(t, err)
		assert.Zero(t, start)

		remaining, err := store.ReadOffsets(ctx, testProjection, 1, 10)
		require.NoError(t, err)
		assert.Empty(t, remaining)
	})
}

// TestSharedPool runs the four stores and the application's own statements on
// one pool, including a transaction that applies an effect and records its
// offset with WriteOffsetsTx, as a projection.TransactionalHandler does.
func TestSharedPool(t *testing.T) {
	ctx := context.Background()
	resetTables(t)

	events := NewEventsStore(testPool)
	snapshots := NewSnapshotStore(testPool)
	states := NewDurableStore(testPool)
	offsets := NewOffsetStore(testPool)

	for _, connector := range []interface{ Connect(context.Context) error }{events, snapshots, states, offsets} {
		require.NoError(t, connector.Connect(ctx))
	}

	require.NoError(t, events.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 100)}))
	require.NoError(t, snapshots.WriteSnapshot(ctx, &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: 1, State: newState(t, "account-1", 10)}))
	require.NoError(t, states.WriteState(ctx, &egopb.DurableState{PersistenceId: "account-2", VersionNumber: 1, ResultingState: newState(t, "account-2", 5)}))

	_, err := testPool.Exec(ctx, "CREATE TABLE IF NOT EXISTS balances (account_id TEXT PRIMARY KEY, balance BIGINT NOT NULL)")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), "DROP TABLE IF EXISTS balances")
	})

	offset := &egopb.EntityOffset{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 100, ConsumedAt: 1000}

	// a rolled back transaction leaves no offset behind
	tx, err := testPool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, offsets.WriteOffsetsTx(ctx, tx, testProjection, []*egopb.EntityOffset{offset}))
	require.NoError(t, tx.Rollback(ctx))

	recorded, err := offsets.ReadEntityOffset(ctx, testProjection, 1, "account-1")
	require.NoError(t, err)
	assert.Nil(t, recorded)

	// a committed transaction stores the effect and the offset together
	tx, err = testPool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "INSERT INTO balances (account_id, balance) VALUES ($1, $2)", "account-1", 10)
	require.NoError(t, err)
	require.NoError(t, offsets.WriteOffsetsTx(ctx, tx, testProjection, []*egopb.EntityOffset{offset}))
	require.NoError(t, tx.Commit(ctx))

	recorded, err = offsets.ReadEntityOffset(ctx, testProjection, 1, "account-1")
	require.NoError(t, err)
	assert.True(t, proto.Equal(offset, recorded))

	latest, err := events.GetLatestEvent(ctx, "account-1")
	require.NoError(t, err)
	assert.NotNil(t, latest)

	for _, disconnector := range []interface{ Disconnect(context.Context) error }{events, snapshots, states, offsets} {
		require.NoError(t, disconnector.Disconnect(ctx))
	}

	require.NoError(t, testPool.Ping(ctx), "the stores must leave the pool open")
}

func TestOffsetStoreFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("on an unreachable database", func(t *testing.T) {
		store := NewOffsetStore(closedPool(t))

		require.Error(t, store.Connect(ctx))
		require.Error(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{{PersistenceId: "account-1"}}))
		require.Error(t, store.DeleteOffsetsBefore(ctx, testProjection, 1, 100, 100))
		require.Error(t, store.ResetOffset(ctx, testProjection, 100))
		require.Error(t, store.DeleteOffset(ctx, testProjection))

		_, err := store.ReadOffsets(ctx, testProjection, 1, 10)
		require.Error(t, err)

		_, err = store.ReadEntityOffset(ctx, testProjection, 1, "account-1")
		require.Error(t, err)

		_, err = store.LatestOffsets(ctx, testProjection)
		require.Error(t, err)

		_, err = store.StartOffset(ctx, testProjection)
		require.Error(t, err)
	})

	t.Run("on a row it cannot read", func(t *testing.T) {
		resetTables(t)
		const insertOffset = "INSERT INTO offsets_store " +
			"(projection_name, shard_number, timestamp, persistence_id, sequence_number, consumed_at) " +
			"VALUES ($1, $2, 100, 'account-1', $3, 100)"

		// a negative shard or sequence number does not fit the unsigned fields of an offset
		insertRow(t, insertOffset, testProjection, 1, -1)
		insertRow(t, insertOffset, testProjection, -1, 1)

		store := NewOffsetStore(testPool)
		_, err := store.ReadOffsets(ctx, testProjection, 1, 10)
		require.Error(t, err)

		_, err = store.LatestOffsets(ctx, testProjection)
		require.Error(t, err)
	})
}
