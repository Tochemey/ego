package projection

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/ego/egopb"
	memeventstore "github.com/tochemey/ego/eventstore/memory"
	"github.com/tochemey/ego/offsetstore"
	"github.com/tochemey/ego/offsetstore/memory"
	testpb "github.com/tochemey/ego/test/data/pb/v1"
	"github.com/tochemey/goakt/log"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProjection(t *testing.T) {
	t.Run("with happy path", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		logger := log.DefaultLogger

		// set up the event store
		journalStore := memeventstore.NewEventsStore()
		assert.NotNil(t, journalStore)

		// set up the offset store
		offsetStore := memory.NewOffsetStore()
		assert.NotNil(t, offsetStore)

		// set up the projection
		// create a handler that return successfully
		handler := func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, sequenceNumber uint64) error {
			return nil
		}

		projection := New(projectionName, handler, journalStore, offsetStore, NewRecovery(), logger)
		// start the projection
		err := projection.Start(ctx)
		require.NoError(t, err)

		// persist some events
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		count := 10
		timestamp := timestamppb.Now()
		journals := make([]*egopb.Event, count)
		for i := 0; i < count; i++ {
			seqNr := i + 1
			journals[i] = &egopb.Event{
				PersistenceId:  persistenceID,
				SequenceNumber: uint64(seqNr),
				IsDeleted:      false,
				Event:          event,
				ResultingState: state,
				Timestamp:      timestamp.AsTime().Unix(),
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.isStarted.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(time.Second)

		// let us grab the current offset
		actual, err := offsetStore.GetCurrentOffset(ctx, offsetstore.NewProjectionID(projectionName, persistenceID))
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.EqualValues(t, 10, actual.GetCurrentOffset())

		// free resources
		assert.NoError(t, projection.Stop(ctx))
	})
	t.Run("with failed handler with fail strategy", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		logger := log.DefaultLogger

		// set up the event store
		journalStore := memeventstore.NewEventsStore()
		assert.NotNil(t, journalStore)

		// set up the offset store
		offsetStore := memory.NewOffsetStore()
		assert.NotNil(t, offsetStore)

		// set up the projection
		// create a handler that return successfully
		handler := func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, sequenceNumber uint64) error {
			return errors.New("damn")
		}

		projection := New(projectionName, handler, journalStore, offsetStore, NewRecovery(), logger)
		// start the projection
		err := projection.Start(ctx)
		require.NoError(t, err)

		// persist some events
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		count := 10
		timestamp := timestamppb.Now()
		journals := make([]*egopb.Event, count)
		for i := 0; i < count; i++ {
			seqNr := i + 1
			journals[i] = &egopb.Event{
				PersistenceId:  persistenceID,
				SequenceNumber: uint64(seqNr),
				IsDeleted:      false,
				Event:          event,
				ResultingState: state,
				Timestamp:      timestamp.AsTime().Unix(),
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.isStarted.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(time.Second)

		// here due to the default recovery strategy the projection is stopped
		require.False(t, projection.isStarted.Load())
		// free resources
		assert.NoError(t, projection.Stop(ctx))
	})
	t.Run("with failed handler and retry_fail strategy", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		logger := log.DefaultLogger

		// set up the event store
		journalStore := memeventstore.NewEventsStore()
		assert.NotNil(t, journalStore)

		// set up the offset store
		offsetStore := memory.NewOffsetStore()
		assert.NotNil(t, offsetStore)

		// set up the projection
		// create a handler that return successfully
		handler := func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, sequenceNumber uint64) error {
			return errors.New("damn")
		}

		projection := New(projectionName, handler, journalStore, offsetStore, NewRecovery(
			WithRecoveryStrategy(egopb.ProjectionRecoveryStrategy_RETRY_AND_FAIL),
			WithRetries(2),
			WithRetryDelay(100*time.Millisecond)), logger)

		// start the projection
		err := projection.Start(ctx)
		require.NoError(t, err)

		// persist some events
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		count := 10
		timestamp := timestamppb.Now()
		journals := make([]*egopb.Event, count)
		for i := 0; i < count; i++ {
			seqNr := i + 1
			journals[i] = &egopb.Event{
				PersistenceId:  persistenceID,
				SequenceNumber: uint64(seqNr),
				IsDeleted:      false,
				Event:          event,
				ResultingState: state,
				Timestamp:      timestamp.AsTime().Unix(),
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.isStarted.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(1 * time.Second)

		// let us grab the current offset
		require.False(t, projection.isStarted.Load())

		// free resources
		assert.NoError(t, projection.Stop(ctx))
	})
	t.Run("with failed handler and skip strategy", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		logger := log.DefaultLogger

		// set up the event store
		journalStore := memeventstore.NewEventsStore()
		assert.NotNil(t, journalStore)

		// set up the offset store
		offsetStore := memory.NewOffsetStore()
		assert.NotNil(t, offsetStore)

		// set up the projection
		// create a handler that return successfully
		handler := func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, sequenceNumber uint64) error {
			if (int(sequenceNumber) % 2) == 0 {
				return errors.New("failed handler")
			}
			return nil
		}

		projection := New(projectionName, handler, journalStore, offsetStore, NewRecovery(
			WithRecoveryStrategy(egopb.ProjectionRecoveryStrategy_SKIP),
			WithRetries(2),
			WithRetryDelay(100*time.Millisecond)), logger)
		// start the projection
		err := projection.Start(ctx)
		require.NoError(t, err)

		// persist some events
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		count := 10
		timestamp := timestamppb.Now()
		journals := make([]*egopb.Event, count)
		for i := 0; i < count; i++ {
			seqNr := i + 1
			journals[i] = &egopb.Event{
				PersistenceId:  persistenceID,
				SequenceNumber: uint64(seqNr),
				IsDeleted:      false,
				Event:          event,
				ResultingState: state,
				Timestamp:      timestamp.AsTime().Unix(),
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.isStarted.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(time.Second)

		// let us grab the current offset
		actual, err := offsetStore.GetCurrentOffset(ctx, offsetstore.NewProjectionID(projectionName, persistenceID))
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.EqualValues(t, 10, actual.GetCurrentOffset())

		// free resource
		assert.NoError(t, projection.Stop(ctx))
	})
	t.Run("with failed handler and skip retry strategy", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		logger := log.DefaultLogger

		// set up the event store
		journalStore := memeventstore.NewEventsStore()
		assert.NotNil(t, journalStore)

		// set up the offset store
		offsetStore := memory.NewOffsetStore()
		assert.NotNil(t, offsetStore)

		// set up the projection
		// create a handler that return successfully
		handler := func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, sequenceNumber uint64) error {
			if (int(sequenceNumber) % 2) == 0 {
				return errors.New("failed handler")
			}
			return nil
		}

		projection := New(projectionName, handler, journalStore, offsetStore, NewRecovery(
			WithRecoveryStrategy(egopb.ProjectionRecoveryStrategy_RETRY_AND_SKIP),
			WithRetries(2),
			WithRetryDelay(100*time.Millisecond)), logger)
		// start the projection
		err := projection.Start(ctx)
		require.NoError(t, err)

		// persist some events
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		count := 10
		timestamp := timestamppb.Now()
		journals := make([]*egopb.Event, count)
		for i := 0; i < count; i++ {
			seqNr := i + 1
			journals[i] = &egopb.Event{
				PersistenceId:  persistenceID,
				SequenceNumber: uint64(seqNr),
				IsDeleted:      false,
				Event:          event,
				ResultingState: state,
				Timestamp:      timestamp.AsTime().Unix(),
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.isStarted.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(time.Second)

		// let us grab the current offset
		actual, err := offsetStore.GetCurrentOffset(ctx, offsetstore.NewProjectionID(projectionName, persistenceID))
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.EqualValues(t, 10, actual.GetCurrentOffset())

		// free resource
		assert.NoError(t, projection.Stop(ctx))
	})
}
