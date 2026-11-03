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

package dynamodb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
)

// offsetProjection is the projection whose offsets the tests write.
const offsetProjection = "account-balances"

func TestOffsetStoreWrites(t *testing.T) {
	ctx := context.Background()

	t.Run("writes more offsets than one transaction holds, each once", func(t *testing.T) {
		store := NewOffsetStore(createOffsetsTable(t), testClient)

		offsets := make([]*egopb.EntityOffset, 0, 250)
		for index := range 250 {
			offsets = append(offsets, newEntityOffset(1, fmt.Sprintf("account-%d", index), 1, int64(index+1)*10))
		}

		rand.Shuffle(len(offsets), func(i, j int) { offsets[i], offsets[j] = offsets[j], offsets[i] })

		// the same offset twice in one write is stored once
		require.NoError(t, store.WriteOffsets(ctx, offsetProjection, append(offsets, offsets[0])))

		read, err := store.ReadOffsets(ctx, offsetProjection, 1, 300)
		require.NoError(t, err)
		require.Len(t, read, 250)
		for index := 1; index < len(read); index++ {
			assert.Greater(t, read[index-1].GetTimestamp(), read[index].GetTimestamp())
		}
	})

	t.Run("orders the transaction items of the offsets by timestamp, each once", func(t *testing.T) {
		store := NewOffsetStore(createOffsetsTable(t), testClient)
		newer := newEntityOffset(1, "account-1", 2, 200)
		older := newEntityOffset(1, "account-2", 1, 100)

		items := store.TransactWriteItems(offsetProjection, []*egopb.EntityOffset{newer, older, newer})
		require.Len(t, items, 2)
		assert.Equal(t, offsetKey(older), items[0].Put.Item[offsetKeyAttribute].(*types.AttributeValueMemberS).Value)

		// a handler commits them in its own transaction
		_, err := testClient.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		require.NoError(t, err)

		offset, err := store.ReadEntityOffset(ctx, offsetProjection, 1, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(newer, offset), "got %v", offset)
	})

	t.Run("keeps apart projections and persistence IDs whose keys share a prefix", func(t *testing.T) {
		store := NewOffsetStore(createOffsetsTable(t), testClient)

		shorter := newEntityOffset(12, "abc", 1, 100)
		longer := newEntityOffset(2, "abcd", 9, 200)
		require.NoError(t, store.WriteOffsets(ctx, "a", []*egopb.EntityOffset{shorter}))
		require.NoError(t, store.WriteOffsets(ctx, "a1", []*egopb.EntityOffset{longer}))
		require.NoError(t, store.WriteOffsets(ctx, "a", []*egopb.EntityOffset{newEntityOffset(12, "abcd", 9, 300)}))

		offsets, err := store.ReadOffsets(ctx, "a1", 2, 10)
		require.NoError(t, err)
		require.Len(t, offsets, 1)
		assert.True(t, proto.Equal(longer, offsets[0]))

		offset, err := store.ReadEntityOffset(ctx, "a", 12, "abc")
		require.NoError(t, err)
		assert.True(t, proto.Equal(shorter, offset), "got %v", offset)
	})

	t.Run("deletes more offsets than one batch request holds", func(t *testing.T) {
		store := NewOffsetStore(createOffsetsTable(t), testClient)

		offsets := make([]*egopb.EntityOffset, 0, 40)
		for index := range 40 {
			offsets = append(offsets, newEntityOffset(1, fmt.Sprintf("account-%d", index), 1, int64(index+1)))
		}

		require.NoError(t, store.WriteOffsets(ctx, offsetProjection, offsets))
		require.NoError(t, store.DeleteOffsetsBefore(ctx, offsetProjection, 1, 1000, 1000))

		read, err := store.ReadOffsets(ctx, offsetProjection, 1, 100)
		require.NoError(t, err)
		assert.Empty(t, read)
	})
}

func TestOffsetStoreFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("fails on a missing table", func(t *testing.T) {
		store := NewOffsetStore(missingTable, testClient)
		offset := newEntityOffset(1, "account-1", 1, 100)

		assert.Error(t, store.Connect(ctx))
		assert.NoError(t, store.Disconnect(ctx))
		assert.ErrorContains(t, store.WriteOffsets(ctx, offsetProjection, []*egopb.EntityOffset{offset}), "failed to write the offsets")
		assert.ErrorContains(t, store.DeleteOffsetsBefore(ctx, offsetProjection, 1, 1, 1), "failed to delete the offsets")
		assert.ErrorContains(t, store.ResetOffset(ctx, offsetProjection, 1), "failed to reset the offsets")
		assert.ErrorContains(t, store.DeleteOffset(ctx, offsetProjection), "failed to delete the offsets")

		_, err := store.ReadOffsets(ctx, offsetProjection, 1, 10)
		assert.ErrorContains(t, err, "failed to read the offsets")

		_, err = store.ReadEntityOffset(ctx, offsetProjection, 1, "account-1")
		assert.ErrorContains(t, err, "failed to read the entity offset")

		_, err = store.LatestOffsets(ctx, offsetProjection)
		assert.ErrorContains(t, err, "failed to read the latest offsets")

		_, err = store.StartOffset(ctx, offsetProjection)
		assert.ErrorContains(t, err, "failed to read the start offset")
	})

	t.Run("fails to decode malformed offsets", func(t *testing.T) {
		tableName := createOffsetsTable(t)
		store := NewOffsetStore(tableName, testClient)

		malformed := offsetAttributes(offsetProjection, newEntityOffset(1, "account-1", 1, 100))
		malformed[timestampAttribute] = stringValue("not a number")
		_, err := testClient.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(tableName), Item: malformed})
		require.NoError(t, err)

		_, err = testClient.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				offsetPartitionAttribute: stringValue(startPartition(offsetProjection)),
				offsetKeyAttribute:       stringValue(startOffsetKey),
				startOffsetAttribute:     stringValue("not a number"),
			},
		})
		require.NoError(t, err)

		_, err = store.ReadOffsets(ctx, offsetProjection, 1, 10)
		assert.ErrorContains(t, err, "failed to decode the offsets")

		_, err = store.ReadEntityOffset(ctx, offsetProjection, 1, "account-1")
		assert.ErrorContains(t, err, "failed to decode the offsets")

		_, err = store.LatestOffsets(ctx, offsetProjection)
		assert.ErrorContains(t, err, "failed to read the latest offsets")

		_, err = store.StartOffset(ctx, offsetProjection)
		assert.ErrorContains(t, err, "failed to decode the start offset")
	})
}

// newEntityOffset builds the offset of a persistence ID's event, consumed at its timestamp.
func newEntityOffset(shardNumber uint64, persistenceID string, sequenceNumber uint64, timestamp int64) *egopb.EntityOffset {
	return &egopb.EntityOffset{
		ShardNumber:    shardNumber,
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Timestamp:      timestamp,
		ConsumedAt:     timestamp,
	}
}
