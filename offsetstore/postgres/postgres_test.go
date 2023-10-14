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

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/offsetstore"
	"github.com/tochemey/gopack/postgres"
	"google.golang.org/protobuf/proto"
)

func TestPostgresOffsetStore(t *testing.T) {
	t.Run("testNewOffsetStore", func(t *testing.T) {
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}

		estore := NewOffsetStore(config)
		assert.NotNil(t, estore)
		var p interface{} = estore
		_, ok := p.(offsetstore.OffsetStore)
		assert.True(t, ok)
	})
	t.Run("testConnect:happy path", func(t *testing.T) {
		ctx := context.TODO()
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
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
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     "testDatabase",
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}

		store := NewOffsetStore(config)
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to ping database connection: pq: database \"testDatabase\" does not exist")
	})
	t.Run("testWriteOffset", func(t *testing.T) {
		ctx := context.TODO()
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
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
			CurrentOffset:  int64(10),
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
}
