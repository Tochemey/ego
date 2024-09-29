/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
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

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/offsetstore"
)

func TestPostgresOffsetStore(t *testing.T) {
	t.Run("testNewOffsetStore", func(t *testing.T) {
		config := &Config{
			testContainer.Host(),
			testContainer.Port(),
			testDatabase,
			testUser,
			testDatabasePassword,
			testContainer.Schema(),
		}

		estore := NewOffsetStore(config)
		assert.NotNil(t, estore)
		var p interface{} = estore
		_, ok := p.(offsetstore.OffsetStore)
		assert.True(t, ok)
	})
	t.Run("testConnect:happy path", func(t *testing.T) {
		ctx := context.TODO()
		config := &Config{
			testContainer.Host(),
			testContainer.Port(),
			testDatabase,
			testUser,
			testDatabasePassword,
			testContainer.Schema(),
		}

		store := NewOffsetStore(config)
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		assert.NoError(t, err)
		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testConnect:database does not exist", func(t *testing.T) {
		ctx := context.TODO()
		config := &Config{
			testContainer.Host(),
			testContainer.Port(),
			"testDatabase",
			testUser,
			testDatabasePassword,
			testContainer.Schema(),
		}

		store := NewOffsetStore(config)
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		assert.Error(t, err)
	})
	t.Run("testWriteOffset", func(t *testing.T) {
		ctx := context.TODO()
		config := &Config{
			testContainer.Host(),
			testContainer.Port(),
			testDatabase,
			testUser,
			testDatabasePassword,
			testContainer.Schema(),
		}

		db, err := dbHandle(ctx)
		require.NoError(t, err)
		schemaUtil := NewSchemaUtils(db)
		err = schemaUtil.CreateTable(ctx)
		require.NoError(t, err)

		store := NewOffsetStore(config)
		assert.NotNil(t, store)
		err = store.Connect(ctx)
		require.NoError(t, err)

		offset := &egopb.Offset{
			ShardNumber:    uint64(9),
			ProjectionName: "some-projection",
			Value:          int64(10),
			Timestamp:      time.Now().UnixMilli(),
		}

		// write the offset
		assert.NoError(t, store.WriteOffset(ctx, offset))

		// get the current shard offset
		current, err := store.GetCurrentOffset(ctx, &egopb.ProjectionId{
			ProjectionName: "some-projection",
			ShardNumber:    uint64(9),
		})
		require.NoError(t, err)
		assert.True(t, proto.Equal(offset, current))

		err = schemaUtil.DropTable(ctx)
		assert.NoError(t, err)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testResetOffset", func(t *testing.T) {
		ctx := context.TODO()
		config := &Config{
			testContainer.Host(),
			testContainer.Port(),
			testDatabase,
			testUser,
			testDatabasePassword,
			testContainer.Schema(),
		}

		db, err := dbHandle(ctx)
		require.NoError(t, err)
		schemaUtil := NewSchemaUtils(db)
		err = schemaUtil.CreateTable(ctx)
		require.NoError(t, err)

		store := NewOffsetStore(config)
		assert.NotNil(t, store)
		err = store.Connect(ctx)
		require.NoError(t, err)

		projection1 := "projection-1"
		projection2 := "projection-2"
		ts := time.Now().UnixMilli()
		// write offset into 10 shards for projection1
		for i := 0; i < 10; i++ {
			shard := uint64(i + 1)
			offset := &egopb.Offset{
				ShardNumber:    shard,
				ProjectionName: projection1,
				Value:          int64(i + 1),
				Timestamp:      ts,
			}
			// write the offset
			assert.NoError(t, store.WriteOffset(ctx, offset))
		}

		// write offset into 10 for projection2
		// this only to have multiple records in the storage
		for i := 0; i < 10; i++ {
			shard := uint64(i + 1)
			offset := &egopb.Offset{
				ShardNumber:    shard,
				ProjectionName: projection2,
				Value:          2 * int64(i+1),
				Timestamp:      ts,
			}
			// write the offset
			assert.NoError(t, store.WriteOffset(ctx, offset))
		}

		// get the current shard offset
		current, err := store.GetCurrentOffset(ctx, &egopb.ProjectionId{
			ProjectionName: projection1,
			ShardNumber:    uint64(9),
		})
		require.NoError(t, err)
		expected := &egopb.Offset{
			ShardNumber:    uint64(9),
			ProjectionName: projection1,
			Value:          int64(9),
			Timestamp:      ts,
		}
		assert.True(t, proto.Equal(expected, current))

		// reset the projection 1 offset
		require.NoError(t, store.ResetOffset(ctx, projection1, int64(1000)))
		current, err = store.GetCurrentOffset(ctx, &egopb.ProjectionId{
			ProjectionName: projection1,
			ShardNumber:    uint64(9),
		})
		require.NoError(t, err)
		assert.EqualValues(t, 1000, current.GetValue())

		err = schemaUtil.DropTable(ctx)
		assert.NoError(t, err)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
}
