/*
 * Copyright (c) 2022-2023 Tochemey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/offsetstore"
	"google.golang.org/protobuf/proto"
)

func TestOffsetStore(t *testing.T) {
	t.Run("testNew", func(t *testing.T) {
		store := NewOffsetStore()
		assert.NotNil(t, store)
		var p interface{} = store
		_, ok := p.(offsetstore.OffsetStore)
		assert.True(t, ok)
	})
	t.Run("testConnect", func(t *testing.T) {
		ctx := context.TODO()
		store := NewOffsetStore()
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testWriteOffset", func(t *testing.T) {
		ctx := context.TODO()

		store := NewOffsetStore()
		assert.NotNil(t, store)
		require.NoError(t, store.Connect(ctx))

		shardNumber := uint64(9)
		projectionName := "DB_WRITER"
		timestamp := time.Now().UnixMilli()

		offset := &egopb.Offset{
			ShardNumber:    shardNumber,
			ProjectionName: projectionName,
			CurrentOffset:  15,
			Timestamp:      timestamp,
		}

		require.NoError(t, store.WriteOffset(ctx, offset))

		err := store.Disconnect(ctx)
		assert.NoError(t, err)
	})

	t.Run("testGetCurrentOffset: happy path", func(t *testing.T) {
		ctx := context.TODO()

		store := NewOffsetStore()
		assert.NotNil(t, store)
		require.NoError(t, store.Connect(ctx))

		shardNumber := uint64(9)
		projectionName := "DB_WRITER"
		timestamp := time.Now().UnixMilli()

		offset := &egopb.Offset{
			ShardNumber:    shardNumber,
			ProjectionName: projectionName,
			CurrentOffset:  15,
			Timestamp:      timestamp,
		}

		require.NoError(t, store.WriteOffset(ctx, offset))

		offset = &egopb.Offset{
			ShardNumber:    shardNumber,
			ProjectionName: projectionName,
			CurrentOffset:  24,
			Timestamp:      timestamp,
		}

		require.NoError(t, store.WriteOffset(ctx, offset))

		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}

		actual, err := store.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.True(t, proto.Equal(offset, actual))

		assert.NoError(t, store.Disconnect(ctx))
	})
	t.Run("testGetCurrentOffset: not found", func(t *testing.T) {
		ctx := context.TODO()

		store := NewOffsetStore()
		assert.NotNil(t, store)
		require.NoError(t, store.Connect(ctx))

		shardNumber := uint64(9)
		projectionName := "DB_WRITER"
		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}
		actual, err := store.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.Nil(t, actual)

		assert.NoError(t, store.Disconnect(ctx))
	})
}
