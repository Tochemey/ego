/*
 * MIT License
 *
 * Copyright (c) 2022-2025 Arsene Tochemey Gandote
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

package ego

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v3/log"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/internal/pause"
	mocksoffsetstore "github.com/tochemey/ego/v3/mocks/offsetstore"
	mockseventstore "github.com/tochemey/ego/v3/mocks/persistence"
	"github.com/tochemey/ego/v3/projection"
	testpb "github.com/tochemey/ego/v3/test/data/testpb"
	testkit2 "github.com/tochemey/ego/v3/testkit"
)

func TestRunner(t *testing.T) {
	t.Run("with happy path", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		logger := log.DiscardLogger

		// set up the event store
		eventsStore := testkit2.NewEventsStore()
		assert.NotNil(t, eventsStore)
		require.NoError(t, eventsStore.Connect(ctx))

		// set up the offset store
		offsetStore := testkit2.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// set up the projection
		// create a underlying that return successfully
		handler := projection.NewDiscardHandler()

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond), withLogger(logger))
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
		require.True(t, runner.running.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		pause.For(time.Second)

		// create the projection id
		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}

		// let us grab the current offset
		actual, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.EqualValues(t, journals[9].GetTimestamp(), actual.GetValue())

		// free resources
		require.NoError(t, eventsStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, runner.Stop())
	})
	t.Run("with failed handler with fail strategy", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()

		// set up the event store
		journalStore := testkit2.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := testkit2.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Disconnect(ctx))

		// set up the projection
		// create a underlying that return successfully
		handler := &testHandler1{}

		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore, withPullInterval(time.Millisecond))
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
		require.True(t, runner.running.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		pause.For(time.Second)

		// here due to the default recovery strategy the projection is stopped
		require.False(t, runner.running.Load())
		// free resources
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with failed handler and retry_fail strategy", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()

		// set up the event store
		journalStore := testkit2.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := testkit2.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Disconnect(ctx))

		// set up the projection
		// create a underlying that return successfully
		handler := &testHandler1{}

		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore,
			withPullInterval(time.Millisecond),
			withRecoveryStrategy(projection.NewRecovery(
				projection.WithRecoveryPolicy(projection.RetryAndFail),
				projection.WithRetries(2),
				projection.WithRetryDelay(100*time.Millisecond))))

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
		for i := range count {
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
		require.True(t, runner.running.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		pause.For(1 * time.Second)

		// let us grab the current offset
		require.False(t, runner.running.Load())

		// free resources
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with failed handler and skip strategy", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shard := uint64(8)

		// set up the event store
		journalStore := testkit2.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := testkit2.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// set up the projection
		// create a underlying that return successfully
		handler := &testHandler2{counter: atomic.NewInt32(0)}

		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore,
			withPullInterval(time.Millisecond),
			withRecoveryStrategy(projection.NewRecovery(
				projection.WithRecoveryPolicy(projection.Skip),
				projection.WithRetries(2),
				projection.WithRetryDelay(100*time.Millisecond))))
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
		for i := range count {
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
		require.True(t, runner.running.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		pause.For(time.Second)

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
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shard := uint64(7)

		// set up the event store
		journalStore := testkit2.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := testkit2.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// set up the projection
		// create a underlying that return successfully
		handler := &testHandler2{counter: atomic.NewInt32(0)}

		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore,
			withPullInterval(time.Millisecond),
			withRecoveryStrategy(projection.NewRecovery(
				projection.WithRecoveryPolicy(projection.RetryAndSkip),
				projection.WithRetries(2),
				projection.WithRetryDelay(100*time.Millisecond))))
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
		for i := range count {
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
		require.True(t, runner.running.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		pause.For(time.Second)

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
		handler := projection.NewDiscardHandler()
		projectionName := "db-writer"
		// set up the offset store
		offsetStore := testkit2.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, nil, offsetStore, withPullInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "events store is not defined")
		assert.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, runner.Stop())
	})
	t.Run("with offset store is not defined", func(t *testing.T) {
		ctx := context.Background()
		handler := projection.NewDiscardHandler()
		projectionName := "db-writer"
		// set up the event store
		eventsStore := testkit2.NewEventsStore()
		assert.NotNil(t, eventsStore)
		require.NoError(t, eventsStore.Connect(ctx))

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, nil, withPullInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "offsets store is not defined")
		assert.NoError(t, eventsStore.Disconnect(ctx))
		require.NoError(t, runner.Stop())
	})
	t.Run("with start when already started returns nil", func(t *testing.T) {
		ctx := context.Background()
		handler := projection.NewDiscardHandler()
		projectionName := "db-writer"
		// set up the event store
		eventsStore := testkit2.NewEventsStore()
		assert.NotNil(t, eventsStore)
		require.NoError(t, eventsStore.Connect(ctx))

		// set up the offset store
		offsetStore := testkit2.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		require.NoError(t, runner.Start(ctx))

		// free resources
		assert.NoError(t, eventsStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with start when max retry to ping events store fails", func(t *testing.T) {
		ctx := context.Background()
		handler := projection.NewDiscardHandler()
		projectionName := "db-writer"

		// set up the offset store
		offsetStore := testkit2.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(errors.New("fail ping"))

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
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
		handler := projection.NewDiscardHandler()
		projectionName := "db-writer"

		// set up the event store
		eventsStore := testkit2.NewEventsStore()
		assert.NotNil(t, eventsStore)
		require.NoError(t, eventsStore.Connect(ctx))

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(errors.New("fail ping"))

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
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
		handler := projection.NewDiscardHandler()
		projectionName := "db-writer"

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)

		resetOffsetTo := time.Now().UTC()
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().ResetOffset(ctx, projectionName, resetOffsetTo.UnixMilli()).Return(errors.New("fail to reset offset"))

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
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
	t.Run("when fail to write the offset stops the projectionRunner", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		timestamp := timestamppb.Now()
		handler := projection.NewDiscardHandler()

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
		offsetStore.EXPECT().WriteOffset(mock.Anything, mock.AnythingOfType("*egopb.Offset")).Return(assert.AnError)

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{shardNumber}, nil)
		eventsStore.EXPECT().GetShardEvents(mock.Anything, shardNumber, offset.GetValue(), uint64(maxBufferSize)).Return(events, nextOffsetValue.AsTime().UnixMilli(), nil)

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
		runner.resetOffsetTo = resetOffsetTo
		runner.maxBufferSize = maxBufferSize

		// start the projection
		err = runner.Start(ctx)
		require.NoError(t, err)

		// run the projection
		runner.Run(ctx)

		pause.For(time.Second)

		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)

		assert.False(t, runner.running.Load())

		require.NoError(t, runner.Stop())
	})
	t.Run("when fail to fetch shard numbers stops the projectionRunner", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		handler := projection.NewDiscardHandler()

		maxBufferSize := 10
		resetOffsetTo := time.Now().UTC()

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().ResetOffset(mock.Anything, projectionName, resetOffsetTo.UnixMilli()).Return(nil)

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return(nil, assert.AnError)

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
		runner.resetOffsetTo = resetOffsetTo
		runner.maxBufferSize = maxBufferSize

		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)

		// run the projection
		runner.Run(ctx)

		pause.For(time.Second)

		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)

		assert.False(t, runner.running.Load())

		require.NoError(t, runner.Stop())
	})
	t.Run("when fail to get current offset stops the projectionRunner", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		shardNumber := uint64(9)

		handler := projection.NewDiscardHandler()

		// create the projection id
		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}

		maxBufferSize := 10
		resetOffsetTo := time.Now().UTC()

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().ResetOffset(mock.Anything, projectionName, resetOffsetTo.UnixMilli()).Return(nil)
		offsetStore.EXPECT().GetCurrentOffset(mock.Anything, projectionID).Return(nil, assert.AnError)

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{shardNumber}, nil)

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
		runner.resetOffsetTo = resetOffsetTo
		runner.maxBufferSize = maxBufferSize

		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)

		// run the projection
		runner.Run(ctx)

		pause.For(time.Second)

		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)

		assert.False(t, runner.running.Load())

		require.NoError(t, runner.Stop())
	})
	t.Run("when fail to get shard events stops the projectionRunner", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"

		shardNumber := uint64(9)
		timestamp := timestamppb.Now()
		handler := projection.NewDiscardHandler()

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

		maxBufferSize := 10
		resetOffsetTo := time.Now().UTC()

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().ResetOffset(mock.Anything, projectionName, resetOffsetTo.UnixMilli()).Return(nil)
		offsetStore.EXPECT().GetCurrentOffset(mock.Anything, projectionID).Return(offset, nil)

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{shardNumber}, nil)
		eventsStore.EXPECT().GetShardEvents(mock.Anything, shardNumber, offset.GetValue(), uint64(maxBufferSize)).Return(nil, 0, assert.AnError)

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
		runner.resetOffsetTo = resetOffsetTo
		runner.maxBufferSize = maxBufferSize

		// start the projection
		err := runner.Start(ctx)
		require.NoError(t, err)

		// run the projection
		runner.Run(ctx)

		pause.For(time.Second)

		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)

		assert.False(t, runner.running.Load())

		require.NoError(t, runner.Stop())
	})
	t.Run("when current offset is zero", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		timestamp := timestamppb.Now()
		handler := projection.NewDiscardHandler()

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
		require.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		require.NoError(t, err)
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
		offsetStore.EXPECT().WriteOffset(mock.Anything, mock.AnythingOfType("*egopb.Offset")).Return(nil)

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{shardNumber}, nil)
		eventsStore.EXPECT().GetShardEvents(mock.Anything, shardNumber, offset.GetValue(), uint64(maxBufferSize)).Return(events, nextOffsetValue.AsTime().UnixMilli(), nil)

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
		runner.resetOffsetTo = resetOffsetTo
		runner.maxBufferSize = maxBufferSize

		// start the projection
		err = runner.Start(ctx)
		require.NoError(t, err)

		// run the projection
		runner.Run(ctx)

		pause.For(time.Second)

		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)

		require.True(t, runner.running.Load())

		require.NoError(t, runner.Stop())
	})
}

type testHandler1 struct{}

var _ projection.Handler = &testHandler1{}

func (x testHandler1) Handle(_ context.Context, _ string, _ *anypb.Any, _ *anypb.Any, _ uint64) error {
	return errors.New("damn")
}

type testHandler2 struct {
	counter *atomic.Int32
}

func (x testHandler2) Handle(_ context.Context, _ string, _ *anypb.Any, _ *anypb.Any, revision uint64) error {
	if (int(revision) % 2) == 0 {
		return errors.New("failed underlying")
	}
	x.counter.Inc()
	return nil
}
