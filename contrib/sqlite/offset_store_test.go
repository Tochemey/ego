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

package sqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
)

// projectionName is the projection the offset tests record offsets for.
const projectionName = "balances"

func TestOffsetStoreConnection(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	store := NewOffsetStore(db)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Ping(ctx))
	require.NoError(t, store.Disconnect(ctx))
	require.NoError(t, db.PingContext(ctx))
}

func TestOffsets(t *testing.T) {
	ctx := context.Background()
	store := NewOffsetStore(newTestDB(t))

	offsets := []*egopb.EntityOffset{
		newOffset(1, "account-1", 1, 100, 1000),
		newOffset(1, "account-1", 2, 200, 2000),
		newOffset(1, "account-2", 1, 150, 1500),
		newOffset(2, "account-3", 1, 300, 3000),
	}
	require.NoError(t, store.WriteOffsets(ctx, projectionName, offsets))

	t.Run("an offset stored already is kept once", func(t *testing.T) {
		require.NoError(t, store.WriteOffsets(ctx, projectionName, offsets[:1]))

		read, err := store.ReadOffsets(ctx, projectionName, 1, 10)
		require.NoError(t, err)
		assert.Len(t, read, 3)
	})

	t.Run("the offsets of a shard are read newest first up to the limit", func(t *testing.T) {
		read, err := store.ReadOffsets(ctx, projectionName, 1, 2)
		require.NoError(t, err)
		require.Len(t, read, 2)
		assert.True(t, proto.Equal(offsets[1], read[0]))
		assert.True(t, proto.Equal(offsets[2], read[1]))
	})

	t.Run("a shard or a projection without offsets reads nothing", func(t *testing.T) {
		read, err := store.ReadOffsets(ctx, projectionName, 9, 10)
		require.NoError(t, err)
		assert.Empty(t, read)

		read, err = store.ReadOffsets(ctx, "other", 1, 10)
		require.NoError(t, err)
		assert.Empty(t, read)
	})

	t.Run("an entity's offset is the one with its highest sequence number", func(t *testing.T) {
		offset, err := store.ReadEntityOffset(ctx, projectionName, 1, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(offsets[1], offset))
	})

	t.Run("an entity without offset has none", func(t *testing.T) {
		offset, err := store.ReadEntityOffset(ctx, projectionName, 1, "unknown")
		require.NoError(t, err)
		assert.Nil(t, offset)
	})

	t.Run("the latest offset of every shard is its newest timestamp", func(t *testing.T) {
		latest, err := store.LatestOffsets(ctx, projectionName)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]int64{1: 200, 2: 300}, latest)

		latest, err = store.LatestOffsets(ctx, "other")
		require.NoError(t, err)
		assert.Empty(t, latest)
	})

	t.Run("old offsets are deleted only when both bounds are passed", func(t *testing.T) {
		// account-2 is older than 200 but was consumed at 1500, after the bound
		require.NoError(t, store.DeleteOffsetsBefore(ctx, projectionName, 1, 200, 1200))

		read, err := store.ReadOffsets(ctx, projectionName, 1, 10)
		require.NoError(t, err)
		require.Len(t, read, 2)
		assert.True(t, proto.Equal(offsets[1], read[0]))
		assert.True(t, proto.Equal(offsets[2], read[1]))
	})
}

func TestOffsetsDeleteBeforeKeepsBounds(t *testing.T) {
	ctx := context.Background()
	store := NewOffsetStore(newTestDB(t))

	require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{
		{ShardNumber: 1, PersistenceId: "account-1", SequenceNumber: 1, Timestamp: 200, ConsumedAt: 1000},
		{ShardNumber: 1, PersistenceId: "account-2", SequenceNumber: 1, Timestamp: 100, ConsumedAt: 2000},
	}))

	// account-1 is stamped at the timestamp bound and account-2 consumed at the consumption bound
	require.NoError(t, store.DeleteOffsetsBefore(ctx, projectionName, 1, 200, 2000))

	read, err := store.ReadOffsets(ctx, projectionName, 1, 10)
	require.NoError(t, err)
	assert.Len(t, read, 2)
}

func TestOffsetsReset(t *testing.T) {
	ctx := context.Background()
	store := NewOffsetStore(newTestDB(t))

	start, err := store.StartOffset(ctx, projectionName)
	require.NoError(t, err)
	assert.Zero(t, start, "a projection never reset has no start offset")

	require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{newOffset(1, "account-1", 1, 100, 1000)}))
	require.NoError(t, store.WriteOffsets(ctx, "other", []*egopb.EntityOffset{newOffset(1, "account-1", 1, 100, 1000)}))

	t.Run("a reset deletes the projection's offsets and records its start", func(t *testing.T) {
		require.NoError(t, store.ResetOffset(ctx, projectionName, 500))
		require.NoError(t, store.ResetOffset(ctx, projectionName, 600))

		latest, err := store.LatestOffsets(ctx, projectionName)
		require.NoError(t, err)
		assert.Empty(t, latest)

		start, err := store.StartOffset(ctx, projectionName)
		require.NoError(t, err)
		assert.EqualValues(t, 600, start)

		latest, err = store.LatestOffsets(ctx, "other")
		require.NoError(t, err)
		assert.Len(t, latest, 1, "another projection keeps its offsets")
	})

	t.Run("a deletion removes the offsets and the start of the projection", func(t *testing.T) {
		require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{newOffset(1, "account-1", 2, 700, 7000)}))
		require.NoError(t, store.DeleteOffset(ctx, projectionName))

		latest, err := store.LatestOffsets(ctx, projectionName)
		require.NoError(t, err)
		assert.Empty(t, latest)

		start, err := store.StartOffset(ctx, projectionName)
		require.NoError(t, err)
		assert.Zero(t, start)
	})

	t.Run("deleting a projection without offsets is not an error", func(t *testing.T) {
		assert.NoError(t, store.DeleteOffset(ctx, "unknown"))
	})
}

func TestWriteOffsetsIsAtomic(t *testing.T) {
	ctx := context.Background()

	t.Run("a write larger than one statement is stored in full", func(t *testing.T) {
		store := NewOffsetStore(newTestDB(t))
		offsets := manyOffsets(insertOffsets.rowsPerStatement() + 1)
		require.NoError(t, store.WriteOffsets(ctx, projectionName, offsets))

		read, err := store.ReadOffsets(ctx, projectionName, 1, uint64(len(offsets)))
		require.NoError(t, err)
		assert.Len(t, read, len(offsets))
	})

	t.Run("an empty write stores nothing", func(t *testing.T) {
		store := NewOffsetStore(newTestDB(t))
		assert.NoError(t, store.WriteOffsets(ctx, projectionName, nil))
	})
}

func TestOffsetStoreQueriesUseTheIndexes(t *testing.T) {
	db := newTestDB(t)

	queries := map[string][]any{
		readOffsetsQuery:         {projectionName, 1, 10},
		readEntityOffsetQuery:    {projectionName, 1, "account-1"},
		latestOffsetsQuery:       {projectionName},
		deleteOffsetsBeforeQuery: {projectionName, 1, 100, 100},
		deleteOffsetsQuery:       {projectionName},
		readStartOffsetQuery:     {projectionName},
		deleteStartOffsetQuery:   {projectionName},
	}

	for query, args := range queries {
		plan := queryPlan(t, db, query, args...)
		assert.NotContains(t, plan, "SCAN ", "%s reads a whole table:\n%s", query, plan)
		assert.NotContains(t, plan, "TEMP B-TREE", "%s sorts in memory:\n%s", query, plan)
	}
}

func TestOffsetStoreErrorsAreReported(t *testing.T) {
	ctx := context.Background()
	store := NewOffsetStore(newClosedDB(t))
	offsets := []*egopb.EntityOffset{newOffset(1, "account-1", 1, 100, 1000)}

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.Ping(ctx))
	assert.ErrorContains(t, store.WriteOffsets(ctx, projectionName, offsets), "failed to write the offsets")
	assert.ErrorContains(t, store.WriteOffsets(ctx, projectionName, manyOffsets(insertOffsets.rowsPerStatement()+1)), "failed to write the offsets")
	assert.ErrorContains(t, store.DeleteOffsetsBefore(ctx, projectionName, 1, 1, 1), "failed to delete the offsets")
	assert.ErrorContains(t, store.ResetOffset(ctx, projectionName, 1), "failed to reset the offsets")
	assert.ErrorContains(t, store.DeleteOffset(ctx, projectionName), "failed to delete the offsets")

	_, err := store.ReadOffsets(ctx, projectionName, 1, 10)
	assert.ErrorContains(t, err, "failed to read the offsets")

	_, err = store.ReadEntityOffset(ctx, projectionName, 1, "account-1")
	assert.ErrorContains(t, err, "failed to read the entity offset")

	_, err = store.LatestOffsets(ctx, projectionName)
	assert.ErrorContains(t, err, "failed to read the latest offsets")

	_, err = store.StartOffset(ctx, projectionName)
	assert.ErrorContains(t, err, "failed to read the start offset")
}

func TestOffsetStoreTransactionErrorsAreReported(t *testing.T) {
	ctx := context.Background()

	// a missing table fails the first statement of each transaction, then the second
	for _, table := range []string{"offsets_store", "offsets_start"} {
		t.Run("a failing statement on "+table+" fails the transaction", func(t *testing.T) {
			db := newTestDB(t)
			store := NewOffsetStore(db)

			_, err := db.ExecContext(ctx, "DROP TABLE "+table)
			require.NoError(t, err)

			assert.ErrorContains(t, store.ResetOffset(ctx, projectionName, 1), "failed to reset the offsets")
			assert.ErrorContains(t, store.DeleteOffset(ctx, projectionName), "failed to delete the offsets")
		})
	}
}

// manyOffsets creates count offsets on shard one, one per persistence ID.
func manyOffsets(count int) []*egopb.EntityOffset {
	offsets := make([]*egopb.EntityOffset, count)
	for index := range offsets {
		offsets[index] = newOffset(1, fmt.Sprintf("account-%d", index), 1, int64(index), int64(index))
	}

	return offsets
}
