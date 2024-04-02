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

package memory

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/eventstore"
	testpb "github.com/tochemey/ego/test/data/pb/v1"
)

func TestEventsStore(t *testing.T) {
	t.Run("testNew", func(t *testing.T) {
		eventsStore := NewEventsStore()
		assert.NotNil(t, eventsStore)
		var p interface{} = eventsStore
		_, ok := p.(eventstore.EventsStore)
		assert.True(t, ok)
	})
	t.Run("testConnect", func(t *testing.T) {
		ctx := context.TODO()
		store := NewEventsStore()
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testWriteEvents", func(t *testing.T) {
		ctx := context.TODO()
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		timestamp := timestamppb.Now()
		shardNumber := uint64(9)

		journal := &egopb.Event{
			PersistenceId:  "persistence-1",
			SequenceNumber: 1,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      timestamp.AsTime().Unix(),
			Shard:          shardNumber,
		}

		store := NewEventsStore()
		assert.NotNil(t, store)
		require.NoError(t, store.Connect(ctx))

		err = store.WriteEvents(ctx, []*egopb.Event{journal})
		assert.NoError(t, err)

		// fetch the data we insert back
		actual, err := store.GetLatestEvent(ctx, "persistence-1")
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.True(t, proto.Equal(journal, actual))

		// assert the number of shards
		shards, err := store.ShardNumbers(ctx)
		assert.NoError(t, err)
		assert.Len(t, shards, 1)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testDeleteEvents", func(t *testing.T) {
		ctx := context.TODO()
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		timestamp := timestamppb.Now()

		journal := &egopb.Event{
			PersistenceId:  "persistence-1",
			SequenceNumber: 1,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      timestamp.AsTime().Unix(),
		}

		store := NewEventsStore()
		assert.NotNil(t, store)
		require.NoError(t, store.Connect(ctx))

		err = store.WriteEvents(ctx, []*egopb.Event{journal})
		assert.NoError(t, err)

		// fetch the data we insert back
		actual, err := store.GetLatestEvent(ctx, "persistence-1")
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.True(t, proto.Equal(journal, actual))

		// delete the journal
		err = store.DeleteEvents(ctx, "persistence-1", 2)
		assert.NoError(t, err)

		actual, err = store.GetLatestEvent(ctx, "persistence-1")
		assert.NoError(t, err)
		assert.Nil(t, actual)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testReplayEvents", func(t *testing.T) {
		ctx := context.TODO()
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		timestamp := timestamppb.Now()

		count := 10
		journals := make([]*egopb.Event, count)
		for i := 0; i < count; i++ {
			seqNr := i + 1
			journals[i] = &egopb.Event{
				PersistenceId:  "persistence-1",
				SequenceNumber: uint64(seqNr),
				IsDeleted:      false,
				Event:          event,
				ResultingState: state,
				Timestamp:      timestamp.AsTime().Unix(),
			}
		}

		store := NewEventsStore()
		assert.NotNil(t, store)
		require.NoError(t, store.Connect(ctx))

		err = store.WriteEvents(ctx, journals)
		assert.NoError(t, err)

		from := uint64(3)
		to := uint64(6)

		actual, err := store.ReplayEvents(ctx, "persistence-1", from, to, 6)
		assert.NoError(t, err)
		assert.NotEmpty(t, actual)
		assert.Len(t, actual, 4)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testPersistenceIDs", func(t *testing.T) {
		ctx := context.TODO()
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		timestamp := timestamppb.Now()

		count := 5
		journals := make([]*egopb.Event, count)
		for i := 0; i < count; i++ {
			seqNr := i + 1
			journals[i] = &egopb.Event{
				PersistenceId:  fmt.Sprintf("persistence-%d", i),
				SequenceNumber: uint64(seqNr),
				IsDeleted:      false,
				Event:          event,
				ResultingState: state,
				Timestamp:      timestamp.AsTime().Unix(),
			}
		}

		store := NewEventsStore()
		assert.NotNil(t, store)
		require.NoError(t, store.Connect(ctx))

		err = store.WriteEvents(ctx, journals)
		assert.NoError(t, err)

		expected := []string{
			"persistence-0",
			"persistence-1",
			"persistence-2",
		}

		pageSize := uint64(3)
		pageToken := ""
		actual, nextPageToken, err := store.PersistenceIDs(ctx, pageSize, pageToken)
		assert.NoError(t, err)
		assert.NotEmpty(t, nextPageToken)
		assert.Equal(t, nextPageToken, "persistence-2")
		assert.ElementsMatch(t, expected, actual)

		actual2, nextPageToken2, err := store.PersistenceIDs(ctx, pageSize, nextPageToken)
		expected = []string{
			"persistence-3",
			"persistence-4",
		}

		assert.NoError(t, err)
		assert.NotEmpty(t, nextPageToken2)
		assert.Equal(t, nextPageToken2, "persistence-4")
		assert.ElementsMatch(t, expected, actual2)

		actual3, nextPageToken3, err := store.PersistenceIDs(ctx, pageSize, nextPageToken2)
		assert.NoError(t, err)
		assert.Empty(t, nextPageToken3)
		assert.Empty(t, actual3)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testGetLatestEvent: not found", func(t *testing.T) {
		ctx := context.TODO()

		store := NewEventsStore()
		assert.NotNil(t, store)
		require.NoError(t, store.Connect(ctx))

		// fetch the data we insert back
		actual, err := store.GetLatestEvent(ctx, "persistence-1")
		assert.NoError(t, err)
		assert.Nil(t, actual)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
}
