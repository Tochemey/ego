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

// Package offsetstoretest checks that implementations of
// offsetstore.OffsetStore honor the contract its godoc states.
//
// Call TestOffsetStore from a test of the implementation, with a function that
// returns a new store on empty storage:
//
//	func TestOffsetStoreContract(t *testing.T) {
//	    offsetstoretest.TestOffsetStore(t, func(t *testing.T) offsetstore.OffsetStore {
//	        return mystore.NewOffsetStore(newEmptyDatabase(t))
//	    })
//	}
//
// Every subtest calls that function once, connects the store and disconnects
// it when the subtest ends.
package offsetstoretest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/offsetstore"
)

const (
	// projectionName is the projection whose offsets the contract writes.
	projectionName = "account-balances"
	// otherProjectionName is a second projection, whose offsets must stay apart.
	otherProjectionName = "account-audit"
)

// TestOffsetStore runs the offsetstore.OffsetStore contract against the stores
// newStore returns. newStore must return a store that is not connected yet, on
// storage that holds no offset and no start offset.
func TestOffsetStore(t *testing.T, newStore func(t *testing.T) offsetstore.OffsetStore) {
	t.Run("pings once connected", func(t *testing.T) {
		store := connectedOffsetStore(t, newStore)
		require.NoError(t, store.Ping(context.Background()))
	})

	t.Run("holds nothing for a projection that recorded nothing", func(t *testing.T) {
		ctx := context.Background()
		store := connectedOffsetStore(t, newStore)

		offsets, err := store.ReadOffsets(ctx, projectionName, 1, 10)
		require.NoError(t, err)
		assert.Empty(t, offsets)

		offset, err := store.ReadEntityOffset(ctx, projectionName, 1, "account-1")
		require.NoError(t, err)
		assert.Nil(t, offset)

		latest, err := store.LatestOffsets(ctx, projectionName)
		require.NoError(t, err)
		assert.Empty(t, latest)

		start, err := store.StartOffset(ctx, projectionName)
		require.NoError(t, err)
		assert.Zero(t, start)
	})

	t.Run("reads the offsets of a projection on a shard, newest first, up to the limit", func(t *testing.T) {
		ctx := context.Background()
		store := connectedOffsetStore(t, newStore)

		oldest := newOffset(1, "account-1", 1, 100, 1000)
		middle := newOffset(1, "account-2", 1, 200, 1000)
		newest := newOffset(1, "account-3", 1, 300, 1000)
		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{middle, oldest, newest}))
		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{newOffset(2, "account-4", 1, 400, 1000)}))
		require.NoError(t, store.WriteOffsets(ctx, otherProjectionName, []*egopb.EntityOffset{newOffset(1, "account-5", 1, 500, 1000)}))

		offsets, err := store.ReadOffsets(ctx, projectionName, 1, 10)
		require.NoError(t, err)
		assertOffsets(t, []*egopb.EntityOffset{newest, middle, oldest}, offsets)

		offsets, err = store.ReadOffsets(ctx, projectionName, 1, 2)
		require.NoError(t, err)
		assertOffsets(t, []*egopb.EntityOffset{newest, middle}, offsets)
	})

	t.Run("stores an offset written twice once", func(t *testing.T) {
		ctx := context.Background()
		store := connectedOffsetStore(t, newStore)

		offset := newOffset(1, "account-1", 1, 100, 1000)
		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{offset}))
		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{offset}))

		offsets, err := store.ReadOffsets(ctx, projectionName, 1, 10)
		require.NoError(t, err)
		assertOffsets(t, []*egopb.EntityOffset{offset}, offsets)
	})

	t.Run("reads the offset of a persistence ID with the highest sequence number", func(t *testing.T) {
		ctx := context.Background()
		store := connectedOffsetStore(t, newStore)

		highest := newOffset(1, "account-1", 3, 300, 1000)
		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{
			newOffset(1, "account-1", 1, 100, 1000),
			highest,
			newOffset(1, "account-1", 2, 200, 1000),
			newOffset(1, "account-2", 9, 400, 1000),
		}))

		offset, err := store.ReadEntityOffset(ctx, projectionName, 1, "account-1")
		require.NoError(t, err)
		assertOffsets(t, []*egopb.EntityOffset{highest}, []*egopb.EntityOffset{offset})

		offset, err = store.ReadEntityOffset(ctx, otherProjectionName, 1, "account-1")
		require.NoError(t, err)
		assert.Nil(t, offset)
	})

	t.Run("maps every shard of a projection to its newest timestamp", func(t *testing.T) {
		ctx := context.Background()
		store := connectedOffsetStore(t, newStore)

		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{
			newOffset(1, "account-1", 1, 100, 1000),
			newOffset(1, "account-2", 1, 300, 1000),
			newOffset(2, "account-3", 1, 200, 1000),
		}))
		require.NoError(t, store.WriteOffsets(ctx, otherProjectionName, []*egopb.EntityOffset{newOffset(3, "account-4", 1, 900, 1000)}))

		latest, err := store.LatestOffsets(ctx, projectionName)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]int64{1: 300, 2: 200}, latest)
	})

	t.Run("deletes the offsets before both bounds only", func(t *testing.T) {
		ctx := context.Background()
		store := connectedOffsetStore(t, newStore)

		consumedLater := newOffset(1, "account-2", 1, 100, 3000)
		atTimestampBound := newOffset(1, "account-3", 1, 200, 1000)
		atConsumedBound := newOffset(1, "account-4", 1, 150, 2000)
		otherShard := newOffset(2, "account-5", 1, 100, 1000)
		otherProjection := newOffset(1, "account-6", 1, 100, 1000)

		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{
			newOffset(1, "account-1", 1, 100, 1000),
			consumedLater,
			atTimestampBound,
			atConsumedBound,
			otherShard,
		}))
		require.NoError(t, store.WriteOffsets(ctx, otherProjectionName, []*egopb.EntityOffset{otherProjection}))

		require.NoError(t, store.DeleteOffsetsBefore(ctx, projectionName, 1, 200, 2000))

		offsets, err := store.ReadOffsets(ctx, projectionName, 1, 10)
		require.NoError(t, err)
		assertOffsets(t, []*egopb.EntityOffset{atTimestampBound, atConsumedBound, consumedLater}, offsets)

		offsets, err = store.ReadOffsets(ctx, projectionName, 2, 10)
		require.NoError(t, err)
		assertOffsets(t, []*egopb.EntityOffset{otherShard}, offsets)

		offsets, err = store.ReadOffsets(ctx, otherProjectionName, 1, 10)
		require.NoError(t, err)
		assertOffsets(t, []*egopb.EntityOffset{otherProjection}, offsets)
	})

	t.Run("resets a projection to a start offset", func(t *testing.T) {
		ctx := context.Background()
		store := connectedOffsetStore(t, newStore)

		kept := newOffset(1, "account-2", 1, 100, 1000)
		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{newOffset(1, "account-1", 1, 100, 1000)}))
		require.NoError(t, store.WriteOffsets(ctx, otherProjectionName, []*egopb.EntityOffset{kept}))

		require.NoError(t, store.ResetOffset(ctx, projectionName, 500))
		require.NoError(t, store.ResetOffset(ctx, projectionName, 700))

		offsets, err := store.ReadOffsets(ctx, projectionName, 1, 10)
		require.NoError(t, err)
		assert.Empty(t, offsets)

		start, err := store.StartOffset(ctx, projectionName)
		require.NoError(t, err)
		assert.Equal(t, int64(700), start)

		offsets, err = store.ReadOffsets(ctx, otherProjectionName, 1, 10)
		require.NoError(t, err)
		assertOffsets(t, []*egopb.EntityOffset{kept}, offsets)
	})

	t.Run("deletes the offsets and the start offset of a projection", func(t *testing.T) {
		ctx := context.Background()
		store := connectedOffsetStore(t, newStore)

		require.NoError(t, store.ResetOffset(ctx, projectionName, 500))
		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{newOffset(1, "account-1", 1, 600, 1000)}))

		require.NoError(t, store.DeleteOffset(ctx, projectionName))
		require.NoError(t, store.DeleteOffset(ctx, otherProjectionName), "deleting a projection with no offset is not an error")

		offsets, err := store.ReadOffsets(ctx, projectionName, 1, 10)
		require.NoError(t, err)
		assert.Empty(t, offsets)

		start, err := store.StartOffset(ctx, projectionName)
		require.NoError(t, err)
		assert.Zero(t, start)
	})
}

// connectedOffsetStore creates an offset store with newStore, connects it and
// disconnects it when the test ends.
func connectedOffsetStore(t *testing.T, newStore func(t *testing.T) offsetstore.OffsetStore) offsetstore.OffsetStore {
	t.Helper()

	store := newStore(t)
	require.NoError(t, store.Connect(context.Background()))
	t.Cleanup(func() {
		assert.NoError(t, store.Disconnect(context.Background()))
	})

	return store
}

// newOffset builds the offset of a persistence ID's event.
func newOffset(shardNumber uint64, persistenceID string, sequenceNumber uint64, timestamp, consumedAt int64) *egopb.EntityOffset {
	return &egopb.EntityOffset{
		ShardNumber:    shardNumber,
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Timestamp:      timestamp,
		ConsumedAt:     consumedAt,
	}
}

// assertOffsets asserts the offsets equal the expected ones, in order.
func assertOffsets(t *testing.T, expected, actual []*egopb.EntityOffset) {
	t.Helper()

	require.Len(t, actual, len(expected))
	for index := range expected {
		assert.True(t, proto.Equal(expected[index], actual[index]), "expected %v, got %v", expected[index], actual[index])
	}
}
