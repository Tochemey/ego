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

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/ego/v5/egopb"
)

// projectionName is the projection whose offsets the tests write.
const projectionName = "balances"

func TestOffsetStoreConnection(t *testing.T) {
	ctx := context.Background()
	store := NewOffsetStore(testDB)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Disconnect(ctx))
	require.NoError(t, store.Ping(ctx), "Disconnect must leave the application's handle open")
}

func TestWriteOffsets(t *testing.T) {
	ctx := context.Background()
	store := NewOffsetStore(testDB)

	t.Run("an empty write stores nothing", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteOffsets(ctx, projectionName, nil))

		latest, err := store.LatestOffsets(ctx, projectionName)
		require.NoError(t, err)
		assert.Empty(t, latest)
	})

	t.Run("a write larger than one statement is stored in full", func(t *testing.T) {
		resetTables(t)
		count := insertOffsets.rowsPerStatement() + 1
		offsets := make([]*egopb.EntityOffset, 0, count)
		for index := range count {
			offsets = append(offsets, newOffset(1, fmt.Sprintf("account-%d", index), 1, int64(index)+1, 1))
		}

		require.NoError(t, store.WriteOffsets(ctx, projectionName, offsets))

		read, err := store.ReadOffsets(ctx, projectionName, 1, uint64(count))
		require.NoError(t, err)
		assert.Len(t, read, count)
	})
}

func TestOffsetsLatestUsesALooseIndexScan(t *testing.T) {
	resetTables(t)
	store := NewOffsetStore(testDB)

	// many offsets on a few shards, so that a shard's newest offset is one of many
	const (
		offsetsCount = 2000
		shards       = 4
	)

	offsets := make([]*egopb.EntityOffset, 0, offsetsCount)
	for index := range offsetsCount {
		offsets = append(offsets, newOffset(uint64(index%shards), fmt.Sprintf("account-%d", index), 1, int64(index), 1))
	}

	require.NoError(t, store.WriteOffsets(context.Background(), projectionName, offsets))

	_, err := testDB.ExecContext(context.Background(), "ANALYZE TABLE offsets_store")
	require.NoError(t, err)

	assert.Contains(t, strings.Join(explain(t, latestOffsetsQuery, projectionName), " "), looseIndexScan)
}

func TestOffsetsResetIsAtomic(t *testing.T) {
	resetTables(t)
	ctx := context.Background()
	store := NewOffsetStore(testDB)
	offset := newOffset(1, "account-1", 1, 100, 1)
	require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{offset}))

	// a projection name longer than its column fails the start offset write, after the deletion
	tooLong := strings.Repeat("p", 256)
	require.NoError(t, store.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{offset}))
	assert.ErrorContains(t, store.ResetOffset(ctx, tooLong, 1), "mysql: failed to reset the offsets")

	read, err := store.ReadOffsets(ctx, projectionName, 1, 10)
	require.NoError(t, err)
	assert.Len(t, read, 1)
}

func TestOffsetStoreErrorsAreReported(t *testing.T) {
	ctx := context.Background()
	store := NewOffsetStore(newClosedDB(t))
	offsets := []*egopb.EntityOffset{newOffset(1, "account-1", 1, 1, 1)}

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.Ping(ctx))
	assert.ErrorContains(t, store.WriteOffsets(ctx, projectionName, offsets), "mysql: failed to write the offsets")
	assert.ErrorContains(t, store.DeleteOffsetsBefore(ctx, projectionName, 1, 1, 1), "mysql: failed to delete the offsets")
	assert.ErrorContains(t, store.ResetOffset(ctx, projectionName, 1), "mysql: failed to reset the offsets")
	assert.ErrorContains(t, store.DeleteOffset(ctx, projectionName), "mysql: failed to delete the offsets")

	_, err := store.ReadOffsets(ctx, projectionName, 1, 10)
	assert.ErrorContains(t, err, "mysql: failed to read the offsets")

	_, err = store.ReadEntityOffset(ctx, projectionName, 1, "account-1")
	assert.ErrorContains(t, err, "mysql: failed to read the entity offset")

	_, err = store.LatestOffsets(ctx, projectionName)
	assert.ErrorContains(t, err, "mysql: failed to read the latest offsets")

	_, err = store.StartOffset(ctx, projectionName)
	assert.ErrorContains(t, err, "mysql: failed to read the start offset")
}
