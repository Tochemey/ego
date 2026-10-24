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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/ego/v5/egopb"
)

const (
	// balancesProjection is the projection most offset store tests write to.
	balancesProjection = "balances"
	// auditProjection is a second projection whose offsets must stay untouched.
	auditProjection = "audit"
)

// newConnectedOffsetStore returns a connected OffsetStore disconnected when the test ends.
func newConnectedOffsetStore(t *testing.T) *OffsetStore {
	t.Helper()
	ctx := context.Background()

	store := NewOffsetStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })
	return store
}

// entityOffset builds an offset of shard 1 consumed at the given timestamp.
func entityOffset(persistenceID string, sequenceNumber uint64, timestamp int64) *egopb.EntityOffset {
	return &egopb.EntityOffset{
		ShardNumber:    1,
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Timestamp:      timestamp,
		ConsumedAt:     timestamp,
	}
}

func TestOffsetStoreReadOffsets(t *testing.T) {
	ctx := context.Background()

	t.Run("returns the offsets of the shard, newest first, up to the limit", func(t *testing.T) {
		store := newConnectedOffsetStore(t)
		require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{
			entityOffset("account-1", 1, 10),
			entityOffset("account-2", 1, 30),
			entityOffset("account-3", 1, 20),
			{ShardNumber: 2, PersistenceId: "account-4", SequenceNumber: 1, Timestamp: 40},
		}))

		offsets, err := store.ReadOffsets(ctx, balancesProjection, 1, 2)
		require.NoError(t, err)
		require.Len(t, offsets, 2)
		assert.Equal(t, "account-2", offsets[0].GetPersistenceId())
		assert.Equal(t, "account-3", offsets[1].GetPersistenceId())
	})

	t.Run("returns nothing for a projection without offsets", func(t *testing.T) {
		store := newConnectedOffsetStore(t)

		offsets, err := store.ReadOffsets(ctx, balancesProjection, 1, 10)
		require.NoError(t, err)
		assert.Empty(t, offsets)
	})
}

func TestOffsetStoreWriteOffsets(t *testing.T) {
	ctx := context.Background()
	store := newConnectedOffsetStore(t)

	// the same offset written twice, as a retried write does, is kept once
	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))
	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{
		entityOffset("account-1", 1, 10),
		entityOffset("account-1", 2, 20),
		entityOffset("account-1", 2, 20),
	}))

	offsets, err := store.ReadOffsets(ctx, balancesProjection, 1, 10)
	require.NoError(t, err)
	require.Len(t, offsets, 2)
	assert.EqualValues(t, 2, offsets[0].GetSequenceNumber())
	assert.EqualValues(t, 1, offsets[1].GetSequenceNumber())

	// an offset deleted by the cleanup is stored again when written again
	require.NoError(t, store.DeleteOffsetsBefore(ctx, balancesProjection, 1, 15, 15))
	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))

	offsets, err = store.ReadOffsets(ctx, balancesProjection, 1, 10)
	require.NoError(t, err)
	assert.Len(t, offsets, 2)
}

func TestOffsetStoreReadEntityOffset(t *testing.T) {
	ctx := context.Background()
	store := newConnectedOffsetStore(t)

	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))
	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 3, 30)}))
	require.NoError(t, store.WriteOffsets(ctx, auditProjection, []*egopb.EntityOffset{entityOffset("account-1", 7, 70)}))

	t.Run("returns the offset with the highest sequence number", func(t *testing.T) {
		offset, err := store.ReadEntityOffset(ctx, balancesProjection, 1, "account-1")
		require.NoError(t, err)
		require.NotNil(t, offset)
		assert.EqualValues(t, 3, offset.GetSequenceNumber())
		assert.EqualValues(t, 30, offset.GetTimestamp())
	})

	t.Run("returns nil for a persistence ID without offsets", func(t *testing.T) {
		offset, err := store.ReadEntityOffset(ctx, balancesProjection, 1, "account-2")
		require.NoError(t, err)
		assert.Nil(t, offset)
	})
}

func TestOffsetStoreLatestOffsets(t *testing.T) {
	ctx := context.Background()
	store := newConnectedOffsetStore(t)

	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{
		entityOffset("account-1", 1, 10),
		entityOffset("account-2", 1, 30),
		{ShardNumber: 2, PersistenceId: "account-3", SequenceNumber: 1, Timestamp: 20},
	}))

	latest, err := store.LatestOffsets(ctx, balancesProjection)
	require.NoError(t, err)
	assert.Equal(t, map[uint64]int64{1: 30, 2: 20}, latest)

	latest, err = store.LatestOffsets(ctx, auditProjection)
	require.NoError(t, err)
	assert.Empty(t, latest)
}

func TestOffsetStoreDeleteOffsetsBefore(t *testing.T) {
	ctx := context.Background()
	store := newConnectedOffsetStore(t)

	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{
		{ShardNumber: 1, PersistenceId: "old-and-consumed-long-ago", SequenceNumber: 1, Timestamp: 10, ConsumedAt: 10},
		{ShardNumber: 1, PersistenceId: "old-but-consumed-recently", SequenceNumber: 1, Timestamp: 10, ConsumedAt: 90},
		{ShardNumber: 1, PersistenceId: "recent", SequenceNumber: 1, Timestamp: 60, ConsumedAt: 10},
		{ShardNumber: 1, PersistenceId: "at-the-timestamp-bound", SequenceNumber: 1, Timestamp: 50, ConsumedAt: 10},
		{ShardNumber: 1, PersistenceId: "consumed-at-the-bound", SequenceNumber: 1, Timestamp: 10, ConsumedAt: 50},
		{ShardNumber: 2, PersistenceId: "other-shard", SequenceNumber: 1, Timestamp: 10, ConsumedAt: 10},
	}))

	require.NoError(t, store.DeleteOffsetsBefore(ctx, balancesProjection, 1, 50, 50))

	remaining, err := store.ReadOffsets(ctx, balancesProjection, 1, 10)
	require.NoError(t, err)
	persistenceIDs := make([]string, 0, len(remaining))
	for _, offset := range remaining {
		persistenceIDs = append(persistenceIDs, offset.GetPersistenceId())
	}
	// both bounds are exclusive: an offset exactly on either one is kept
	assert.ElementsMatch(t, []string{"old-but-consumed-recently", "recent", "at-the-timestamp-bound", "consumed-at-the-bound"}, persistenceIDs)

	otherShard, err := store.ReadOffsets(ctx, balancesProjection, 2, 10)
	require.NoError(t, err)
	assert.Len(t, otherShard, 1)
}

// TestOffsetStoreResetOffset asserts that a reset deletes every offset of the
// named projection, records its start offset, and leaves other projections alone.
func TestOffsetStoreResetOffset(t *testing.T) {
	ctx := context.Background()
	store := newConnectedOffsetStore(t)

	start, err := store.StartOffset(ctx, balancesProjection)
	require.NoError(t, err)
	assert.Zero(t, start)

	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))
	require.NoError(t, store.WriteOffsets(ctx, auditProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))

	require.NoError(t, store.ResetOffset(ctx, balancesProjection, 5))

	start, err = store.StartOffset(ctx, balancesProjection)
	require.NoError(t, err)
	assert.EqualValues(t, 5, start)

	latest, err := store.LatestOffsets(ctx, balancesProjection)
	require.NoError(t, err)
	assert.Empty(t, latest)

	latest, err = store.LatestOffsets(ctx, auditProjection)
	require.NoError(t, err)
	assert.Equal(t, map[uint64]int64{1: 10}, latest)

	// an offset written again after the reset is stored again
	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))

	latest, err = store.LatestOffsets(ctx, balancesProjection)
	require.NoError(t, err)
	assert.Equal(t, map[uint64]int64{1: 10}, latest)
}

// TestOffsetStoreDeleteOffset asserts that a deletion drops every offset and the
// start offset of the named projection, leaves other projections alone, and
// accepts a projection that has no offset at all.
func TestOffsetStoreDeleteOffset(t *testing.T) {
	ctx := context.Background()
	store := newConnectedOffsetStore(t)

	require.NoError(t, store.ResetOffset(ctx, balancesProjection, 5))
	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))
	require.NoError(t, store.WriteOffsets(ctx, auditProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))

	require.NoError(t, store.DeleteOffset(ctx, balancesProjection))

	start, err := store.StartOffset(ctx, balancesProjection)
	require.NoError(t, err)
	assert.Zero(t, start)

	latest, err := store.LatestOffsets(ctx, balancesProjection)
	require.NoError(t, err)
	assert.Empty(t, latest)

	latest, err = store.LatestOffsets(ctx, auditProjection)
	require.NoError(t, err)
	assert.Equal(t, map[uint64]int64{1: 10}, latest)

	// an offset written again after the deletion is stored again
	require.NoError(t, store.WriteOffsets(ctx, balancesProjection, []*egopb.EntityOffset{entityOffset("account-1", 1, 10)}))

	latest, err = store.LatestOffsets(ctx, balancesProjection)
	require.NoError(t, err)
	assert.Equal(t, map[uint64]int64{1: 10}, latest)

	require.NoError(t, store.DeleteOffset(ctx, "unknown"))
}
