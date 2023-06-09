package memory

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/eventstore"
	testpb "github.com/tochemey/ego/test/data/pb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventsStore(t *testing.T) {
	t.Run("testNew", func(t *testing.T) {
		estore := NewEventsStore()
		assert.NotNil(t, estore)
		var p interface{} = estore
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
			"persistence-3",
			"persistence-4",
		}

		actual, err := store.PersistenceIDs(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, expected, actual)

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
