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

package cassandra

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
)

const (
	// testProjection is the projection whose offsets the tests write.
	testProjection = "account-balances"
	// offsetsTable creates the offsets_store table, as resources/offsetstore_cassandra.cql does.
	offsetsTable = `CREATE TABLE IF NOT EXISTS offsets_store (projection_name text, shard_number bigint, timestamp bigint, persistence_id text, sequence_number bigint, consumed_at bigint, PRIMARY KEY ((projection_name, shard_number), timestamp, persistence_id, sequence_number)) WITH CLUSTERING ORDER BY (timestamp DESC, persistence_id DESC, sequence_number DESC)`
	// entityOffsetsTable creates the offsets_by_entity table, as resources/offsetstore_cassandra.cql does.
	entityOffsetsTable = `CREATE TABLE IF NOT EXISTS offsets_by_entity (projection_name text, shard_number bigint, persistence_id text, sequence_number bigint, timestamp bigint, consumed_at bigint, PRIMARY KEY ((projection_name, shard_number), persistence_id, sequence_number)) WITH CLUSTERING ORDER BY (persistence_id ASC, sequence_number DESC)`
	// invalidUTF8 is a persistence ID Cassandra rejects in a text column.
	invalidUTF8 = "\xff\xfe"
)

func TestOffsetStore(t *testing.T) {
	ctx := context.Background()
	session := newSession(t, testKeyspace)
	store := NewOffsetStore(session)

	require.NoError(t, store.Connect(ctx))

	t.Run("reads no offset for a limit of zero", func(t *testing.T) {
		truncate(t, session, "offsets_store", "offsets_by_entity", "offsets_start")
		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{newOffset(1, "account-1", 1, 100, 1000)}))

		offsets, err := store.ReadOffsets(ctx, testProjection, 1, 0)
		require.NoError(t, err)
		assert.Empty(t, offsets)
	})

	t.Run("writes more offsets than a batch holds", func(t *testing.T) {
		truncate(t, session, "offsets_store", "offsets_by_entity", "offsets_start")

		count := 2*offsetsPerBatch + 3
		offsets := make([]*egopb.EntityOffset, 0, count)
		for index := range count {
			// written newest first, so the store has to order them
			offsets = append(offsets, newOffset(1, fmt.Sprintf("account-%d", index), 1, int64(count-index)*100, 1000))
		}

		require.NoError(t, store.WriteOffsets(ctx, testProjection, offsets))

		stored, err := store.ReadOffsets(ctx, testProjection, 1, uint64(count))
		require.NoError(t, err)
		require.Len(t, stored, count)
		for index := range offsets {
			assert.True(t, proto.Equal(offsets[index], stored[index]), "expected %v, got %v", offsets[index], stored[index])
		}

		var entities int
		require.NoError(t, session.Query(`SELECT COUNT(*) FROM offsets_by_entity WHERE projection_name = ? AND shard_number = ?`, testProjection, 1).ScanContext(ctx, &entities))
		assert.Equal(t, count, entities)
	})

	t.Run("stores the oldest offsets when a later batch fails", func(t *testing.T) {
		truncate(t, session, "offsets_store", "offsets_by_entity", "offsets_start")

		offsets := make([]*egopb.EntityOffset, 0, offsetsPerBatch+1)
		for index := range offsetsPerBatch {
			offsets = append(offsets, newOffset(1, fmt.Sprintf("account-%d", index), 1, int64(index+1)*100, 1000))
		}

		// Cassandra rejects the newest offset, whose persistence ID is not valid UTF-8, so the second batch fails
		unwritable := newOffset(1, invalidUTF8, 1, int64(offsetsPerBatch+1)*100, 1000)
		require.Error(t, store.WriteOffsets(ctx, testProjection, append([]*egopb.EntityOffset{unwritable}, offsets...)))

		stored, err := store.ReadOffsets(ctx, testProjection, 1, uint64(2*offsetsPerBatch))
		require.NoError(t, err)
		assert.Len(t, stored, offsetsPerBatch)
	})

	t.Run("deletes expired offsets from both tables", func(t *testing.T) {
		truncate(t, session, "offsets_store", "offsets_by_entity", "offsets_start")
		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{
			newOffset(1, "account-1", 1, 100, 1000),
			newOffset(1, "account-2", 1, 300, 1000),
		}))

		require.NoError(t, store.DeleteOffsetsBefore(ctx, testProjection, 1, 200, 2000))

		expired, err := store.ReadEntityOffset(ctx, testProjection, 1, "account-1")
		require.NoError(t, err)
		assert.Nil(t, expired)

		kept, err := store.ReadEntityOffset(ctx, testProjection, 1, "account-2")
		require.NoError(t, err)
		assert.NotNil(t, kept)
	})

	t.Run("resets the offsets of both tables", func(t *testing.T) {
		truncate(t, session, "offsets_store", "offsets_by_entity", "offsets_start")
		require.NoError(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{newOffset(7, "account-1", 1, 100, 1000)}))

		require.NoError(t, store.ResetOffset(ctx, testProjection, 500))

		offset, err := store.ReadEntityOffset(ctx, testProjection, 7, "account-1")
		require.NoError(t, err)
		assert.Nil(t, offset)
	})

	require.NoError(t, store.Disconnect(ctx))
	require.NoError(t, store.Ping(ctx), "Disconnect must leave the application's session open")
}

func TestOffsetStoreErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("fails on a closed session", func(t *testing.T) {
		store := NewOffsetStore(newClosedSession(t))

		assert.Error(t, store.Connect(ctx))
		assert.Error(t, store.Ping(ctx))
		assert.Error(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{newOffset(1, "account-1", 1, 100, 1000)}))
		assert.Error(t, store.DeleteOffsetsBefore(ctx, testProjection, 1, 200, 2000))
		assert.Error(t, store.ResetOffset(ctx, testProjection, 500))
		assert.Error(t, store.DeleteOffset(ctx, testProjection))

		offsets, err := store.ReadOffsets(ctx, testProjection, 1, 10)
		assert.Error(t, err)
		assert.Nil(t, offsets)

		offset, err := store.ReadEntityOffset(ctx, testProjection, 1, "account-1")
		assert.Error(t, err)
		assert.Nil(t, offset)

		latest, err := store.LatestOffsets(ctx, testProjection)
		assert.Error(t, err)
		assert.Nil(t, latest)

		start, err := store.StartOffset(ctx, testProjection)
		assert.Error(t, err)
		assert.Zero(t, start)
	})

	t.Run("fails when the entity table is missing", func(t *testing.T) {
		session := newKeyspace(t, "ego_no_entity_offsets", offsetsTable)
		require.NoError(t, session.Query(insertOffsetStatement, testProjection, 1, 100, "account-1", 1, 1000).ExecContext(ctx))
		store := NewOffsetStore(session)

		assert.Error(t, store.WriteOffsets(ctx, testProjection, []*egopb.EntityOffset{newOffset(1, "account-2", 1, 100, 1000)}))
		assert.Error(t, store.DeleteOffsetsBefore(ctx, testProjection, 1, 200, 2000))
		assert.Error(t, store.ResetOffset(ctx, testProjection, 500))
	})

	t.Run("fails when the start offset table is missing", func(t *testing.T) {
		session := newKeyspace(t, "ego_no_start_offset", offsetsTable, entityOffsetsTable)
		store := NewOffsetStore(session)

		assert.Error(t, store.ResetOffset(ctx, testProjection, 500))
		assert.Error(t, store.DeleteOffset(ctx, testProjection))
	})
}
