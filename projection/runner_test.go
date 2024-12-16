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

package projection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/goakt/v2/log"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/internal/lib"
	mocksoffsetstore "github.com/tochemey/ego/v3/mocks/offsetstore"
	mockseventstore "github.com/tochemey/ego/v3/mocks/persistence"
	memoffsetstore "github.com/tochemey/ego/v3/offsetstore/memory"
	"github.com/tochemey/ego/v3/plugins/eventstore/memory"
	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
)

func TestRunner(t *testing.T) {
	t.Run("with happy path", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		logger := log.DefaultLogger

		// set up the event store
		eventsStore := memory.NewEventsStore()
		assert.NotNil(t, eventsStore)
		require.NoError(t, eventsStore.Connect(ctx))

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// set up the projection
		// create a handler that return successfully
		handler := NewDiscardHandler(logger)

		// create an instance of the projection
		runner := newRunner(projectionName, handler, eventsStore, offsetStore, WithRefreshInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)

		require.Equal(t, projectionName, runner.Name())

		// run the projection
		runner.Run(ctx)

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
				Shard:          shardNumber,
			}
		}

		require.NoError(t, eventsStore.WriteEvents(ctx, journals))
		require.True(t, runner.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		lib.Pause(time.Second)

		// create the projection id
		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}

		// let us grab the current offset
		actual, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.EqualValues(t, journals[9].GetTimestamp(), actual.GetValue())

		assert.EqualValues(t, 10, handler.EventsCount())

		// free resources
		assert.NoError(t, eventsStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with failed handler with fail strategy", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()

		// set up the event store
		journalStore := memory.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Disconnect(ctx))

		// set up the projection
		// create a handler that return successfully
		handler := &testHandler1{}

		runner := newRunner(projectionName, handler, journalStore, offsetStore, WithRefreshInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)

		// run the projection
		runner.Run(ctx)

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
		require.True(t, runner.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		lib.Pause(time.Second)

		// here due to the default recovery strategy the projection is stopped
		require.False(t, runner.started.Load())
		// free resources
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with failed handler and retry_fail strategy", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()

		// set up the event store
		journalStore := memory.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Disconnect(ctx))

		// set up the projection
		// create a handler that return successfully
		handler := &testHandler1{}

		runner := newRunner(projectionName, handler, journalStore, offsetStore,
			WithRefreshInterval(time.Millisecond),
			WithRecoveryStrategy(NewRecovery(
				WithRecoveryPolicy(RetryAndFail),
				WithRetries(2),
				WithRetryDelay(100*time.Millisecond))))

		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)
		// run the projection
		runner.Run(ctx)

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
		require.True(t, runner.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		lib.Pause(1 * time.Second)

		// let us grab the current offset
		require.False(t, runner.started.Load())

		// free resources
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with failed handler and skip strategy", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shard := uint64(8)

		// set up the event store
		journalStore := memory.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// set up the projection
		// create a handler that return successfully
		handler := &testHandler2{counter: atomic.NewInt32(0)}

		runner := newRunner(projectionName, handler, journalStore, offsetStore,
			WithRefreshInterval(time.Millisecond),
			WithRecoveryStrategy(NewRecovery(
				WithRecoveryPolicy(Skip),
				WithRetries(2),
				WithRetryDelay(100*time.Millisecond))))
		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)
		// run the projection
		runner.Run(ctx)
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
				Shard:          shard,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, runner.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		lib.Pause(time.Second)

		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shard,
		}

		// let us grab the current offset
		actual, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.EqualValues(t, 5, handler.counter.Load())

		// free resource
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with failed handler and skip retry strategy", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shard := uint64(7)

		// set up the event store
		journalStore := memory.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// set up the projection
		// create a handler that return successfully
		handler := &testHandler2{counter: atomic.NewInt32(0)}

		runner := newRunner(projectionName, handler, journalStore, offsetStore,
			WithRefreshInterval(time.Millisecond),
			WithRecoveryStrategy(NewRecovery(
				WithRecoveryPolicy(RetryAndSkip),
				WithRetries(2),
				WithRetryDelay(100*time.Millisecond))))
		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)
		// run the projection
		runner.Run(ctx)
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
				Shard:          shard,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, runner.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		lib.Pause(time.Second)

		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shard,
		}

		// let us grab the current offset
		actual, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.EqualValues(t, 5, handler.counter.Load())

		// free resource
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with events store is not defined", func(t *testing.T) {
		ctx := context.Background()
		handler := NewDiscardHandler(log.DiscardLogger)
		projectionName := "db-writer"
		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// create an instance of the projection
		runner := newRunner(projectionName, handler, nil, offsetStore, WithRefreshInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "events store is not defined")
		assert.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, runner.Stop())
	})
	t.Run("with offset store is not defined", func(t *testing.T) {
		ctx := context.Background()
		handler := NewDiscardHandler(log.DiscardLogger)
		projectionName := "db-writer"
		// set up the event store
		eventsStore := memory.NewEventsStore()
		assert.NotNil(t, eventsStore)
		require.NoError(t, eventsStore.Connect(ctx))

		// create an instance of the projection
		runner := newRunner(projectionName, handler, eventsStore, nil, WithRefreshInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "offsets store is not defined")
		assert.NoError(t, eventsStore.Disconnect(ctx))
		require.NoError(t, runner.Stop())
	})
	t.Run("with start when already started returns nil", func(t *testing.T) {
		ctx := context.Background()
		handler := NewDiscardHandler(log.DiscardLogger)
		projectionName := "db-writer"
		// set up the event store
		eventsStore := memory.NewEventsStore()
		assert.NotNil(t, eventsStore)
		require.NoError(t, eventsStore.Connect(ctx))

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// create an instance of the projection
		runner := newRunner(projectionName, handler, eventsStore, offsetStore, WithRefreshInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		require.NoError(t, runner.Start(ctx))

		// free resources
		assert.NoError(t, eventsStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with start when max retry to ping events store fails", func(t *testing.T) {
		ctx := context.Background()
		handler := NewDiscardHandler(log.DiscardLogger)
		projectionName := "db-writer"

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(errors.New("fail ping"))

		// create an instance of the projection
		runner := newRunner(projectionName, handler, eventsStore, offsetStore, WithRefreshInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "failed to start the projection: fail ping")
		eventsStore.AssertExpectations(t)
		assert.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, runner.Stop())
	})
	t.Run("with start when max retry to ping offsets store store fails", func(t *testing.T) {
		ctx := context.Background()
		handler := NewDiscardHandler(log.DiscardLogger)
		projectionName := "db-writer"

		// set up the event store
		eventsStore := memory.NewEventsStore()
		assert.NotNil(t, eventsStore)
		require.NoError(t, eventsStore.Connect(ctx))

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(errors.New("fail ping"))

		// create an instance of the projection
		runner := newRunner(projectionName, handler, eventsStore, offsetStore, WithRefreshInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "failed to start the projection: fail ping")
		offsetStore.AssertExpectations(t)
		assert.NoError(t, eventsStore.Disconnect(ctx))
		require.NoError(t, runner.Stop())
	})
	t.Run("with start when ResetOffset fails", func(t *testing.T) {
		ctx := context.Background()
		handler := NewDiscardHandler(log.DiscardLogger)
		projectionName := "db-writer"

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)

		resetOffsetTo := time.Now().UTC()
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().ResetOffset(ctx, projectionName, resetOffsetTo.UnixMilli()).Return(errors.New("fail to reset offset"))

		// create an instance of the projection
		runner := newRunner(projectionName, handler, eventsStore, offsetStore, WithRefreshInterval(time.Millisecond))
		// purposefully for test
		runner.resetOffsetTo = resetOffsetTo

		// start the projection
		err := runner.Start(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "failed to reset projection=db-writer: fail to reset offset")
		offsetStore.AssertExpectations(t)
		eventsStore.AssertExpectations(t)
		require.NoError(t, runner.Stop())
	})
	t.Run("when fail to write the offset stops the runner", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		timestamp := timestamppb.Now()
		logger := log.DiscardLogger
		handler := NewDiscardHandler(logger)

		// create the projection id
		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}

		offset := &egopb.Offset{
			ShardNumber:    shardNumber,
			ProjectionName: projectionName,
			Value:          timestamp.AsTime().Unix(),
			Timestamp:      0,
		}

		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)
		nextOffsetValue := timestamppb.New(time.Now().Add(time.Minute))
		events := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          event,
				ResultingState: state,
				Timestamp:      timestamp.AsTime().Unix(),
				Shard:          shardNumber,
			},
		}

		maxBufferSize := 10
		resetOffsetTo := time.Now().UTC()

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().ResetOffset(mock.Anything, projectionName, resetOffsetTo.UnixMilli()).Return(nil)
		offsetStore.EXPECT().GetCurrentOffset(mock.Anything, projectionID).Return(offset, nil)
		offsetStore.EXPECT().WriteOffset(mock.Anything, mock.AnythingOfType("*egopb.Offset")).Return(errors.New("fail to write the offset"))

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{shardNumber}, nil)
		eventsStore.EXPECT().GetShardEvents(mock.Anything, shardNumber, offset.GetValue(), uint64(maxBufferSize)).Return(events, nextOffsetValue.AsTime().UnixMilli(), nil)

		// create an instance of the projection
		runner := newRunner(projectionName, handler, eventsStore, offsetStore, WithRefreshInterval(time.Millisecond))
		runner.resetOffsetTo = resetOffsetTo
		runner.maxBufferSize = maxBufferSize

		// start the projection
		err = runner.Start(ctx)
		require.NoError(t, err)

		// run the projection
		runner.Run(ctx)

		lib.Pause(time.Second)

		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)

		assert.False(t, runner.started.Load())

		require.NoError(t, runner.Stop())
	})
}

type testHandler1 struct{}

var _ Handler = &testHandler1{}

func (x testHandler1) Handle(_ context.Context, _ string, _ *anypb.Any, _ *anypb.Any, _ uint64) error {
	return errors.New("damn")
}

type testHandler2 struct {
	counter *atomic.Int32
}

func (x testHandler2) Handle(_ context.Context, _ string, _ *anypb.Any, _ *anypb.Any, revision uint64) error {
	if (int(revision) % 2) == 0 {
		return errors.New("failed handler")
	}
	x.counter.Inc()
	return nil
}
