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
	"github.com/tochemey/goakt/v4/log"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/internal/pause"
	mockencryption "github.com/tochemey/ego/v4/mocks/encryption"
	mockadapter "github.com/tochemey/ego/v4/mocks/eventadapter"
	mocksoffsetstore "github.com/tochemey/ego/v4/mocks/offsetstore"
	mockseventstore "github.com/tochemey/ego/v4/mocks/persistence"
	"github.com/tochemey/ego/v4/projection"
	testpb "github.com/tochemey/ego/v4/test/data/testpb"
	testkit2 "github.com/tochemey/ego/v4/testkit"
)

func TestProjectionRunnerErrorPaths(t *testing.T) {
	t.Run("with decrypt failure in processEnvelope stops the runner", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		timestamp := timestamppb.Now()

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

		// Build a valid anypb payload
		eventProto := &testpb.AccountCredited{}
		eventAny, err := anypb.New(eventProto)
		require.NoError(t, err)

		// Mark event as encrypted so the decrypt path is triggered
		encryptedBytes := []byte("cipher")
		events := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event: &anypb.Any{
					TypeUrl: eventAny.GetTypeUrl(),
					Value:   encryptedBytes,
				},
				Timestamp:       timestamp.AsTime().Unix(),
				Shard:           shardNumber,
				IsEncrypted:     true,
				EncryptionKeyId: "key-1",
			},
		}

		nextOffset := timestamppb.New(time.Now().Add(time.Minute))
		maxBufferSize := 10
		resetOffsetTo := time.Now().UTC()

		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Decrypt(mock.Anything, persistenceID, encryptedBytes, "key-1").Return(nil, assert.AnError)

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().ResetOffset(mock.Anything, projectionName, resetOffsetTo.UnixMilli()).Return(nil)
		offsetStore.EXPECT().GetCurrentOffset(mock.Anything, projectionID).Return(offset, nil)

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{shardNumber}, nil)
		eventsStore.EXPECT().GetShardEvents(mock.Anything, shardNumber, offset.GetValue(), uint64(maxBufferSize)).Return(events, nextOffset.AsTime().UnixMilli(), nil)

		handler := projection.NewDiscardHandler()
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore,
			withPullInterval(time.Millisecond),
			withEncryptor(encryptor),
		)
		runner.resetOffsetTo = resetOffsetTo
		runner.maxBufferSize = maxBufferSize

		err = runner.Start(ctx)
		require.NoError(t, err)

		runner.Run(ctx)

		pause.For(time.Second)

		encryptor.AssertExpectations(t)
		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)

		assert.False(t, runner.running.Load())

		require.NoError(t, runner.Stop())
	})
	t.Run("with unmarshal failure after decrypt in processEnvelope stops the runner", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		timestamp := timestamppb.Now()

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

		eventProto := &testpb.AccountCredited{}
		eventAny, err := anypb.New(eventProto)
		require.NoError(t, err)

		encryptedBytes := []byte("cipher")
		events := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event: &anypb.Any{
					TypeUrl: eventAny.GetTypeUrl(),
					Value:   encryptedBytes,
				},
				Timestamp:       timestamp.AsTime().Unix(),
				Shard:           shardNumber,
				IsEncrypted:     true,
				EncryptionKeyId: "key-1",
			},
		}

		nextOffset := timestamppb.New(time.Now().Add(time.Minute))
		maxBufferSize := 10
		resetOffsetTo := time.Now().UTC()

		// Return invalid bytes that cannot be unmarshalled as a proto message
		invalidBytes := []byte("not-valid-proto")

		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Decrypt(mock.Anything, persistenceID, encryptedBytes, "key-1").Return(invalidBytes, nil)

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().ResetOffset(mock.Anything, projectionName, resetOffsetTo.UnixMilli()).Return(nil)
		offsetStore.EXPECT().GetCurrentOffset(mock.Anything, projectionID).Return(offset, nil)

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{shardNumber}, nil)
		eventsStore.EXPECT().GetShardEvents(mock.Anything, shardNumber, offset.GetValue(), uint64(maxBufferSize)).Return(events, nextOffset.AsTime().UnixMilli(), nil)

		handler := projection.NewDiscardHandler()
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore,
			withPullInterval(time.Millisecond),
			withEncryptor(encryptor),
		)
		runner.resetOffsetTo = resetOffsetTo
		runner.maxBufferSize = maxBufferSize

		err = runner.Start(ctx)
		require.NoError(t, err)

		runner.Run(ctx)

		pause.For(time.Second)

		encryptor.AssertExpectations(t)
		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)

		assert.False(t, runner.running.Load())

		require.NoError(t, runner.Stop())
	})
	t.Run("with event adapter chain failure in processEnvelope stops the runner", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		timestamp := timestamppb.Now()

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

		eventProto := &testpb.AccountCredited{}
		eventAny, err := anypb.New(eventProto)
		require.NoError(t, err)

		events := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          eventAny,
				Timestamp:      timestamp.AsTime().Unix(),
				Shard:          shardNumber,
			},
		}

		nextOffset := timestamppb.New(time.Now().Add(time.Minute))
		maxBufferSize := 10
		resetOffsetTo := time.Now().UTC()

		adapter := new(mockadapter.EventAdapter)
		adapter.EXPECT().Adapt(eventAny, uint64(1)).Return(nil, assert.AnError)

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().ResetOffset(mock.Anything, projectionName, resetOffsetTo.UnixMilli()).Return(nil)
		offsetStore.EXPECT().GetCurrentOffset(mock.Anything, projectionID).Return(offset, nil)

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{shardNumber}, nil)
		eventsStore.EXPECT().GetShardEvents(mock.Anything, shardNumber, offset.GetValue(), uint64(maxBufferSize)).Return(events, nextOffset.AsTime().UnixMilli(), nil)

		handler := projection.NewDiscardHandler()
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore,
			withPullInterval(time.Millisecond),
			withEventAdapters([]eventadapter.EventAdapter{adapter}),
		)
		runner.resetOffsetTo = resetOffsetTo
		runner.maxBufferSize = maxBufferSize

		err = runner.Start(ctx)
		require.NoError(t, err)

		runner.Run(ctx)

		pause.For(time.Second)

		adapter.AssertExpectations(t)
		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)

		assert.False(t, runner.running.Load())

		require.NoError(t, runner.Stop())

	})
}

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

				Timestamp: timestamp.AsTime().Unix(),
				Shard:     shardNumber,
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

				Timestamp: timestamp.AsTime().Unix(),
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

				Timestamp: timestamp.AsTime().Unix(),
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

				Timestamp: timestamp.AsTime().Unix(),
				Shard:     shard,
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

				Timestamp: timestamp.AsTime().Unix(),
				Shard:     shard,
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
	t.Run("with handler panic and fail strategy", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		timestamp := timestamppb.Now()
		handler := &testPanicHandler{}

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

		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)
		nextOffsetValue := timestamppb.New(time.Now().Add(time.Minute))
		events := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          event,

				Timestamp: timestamp.AsTime().Unix(),
				Shard:     shardNumber,
			},
		}

		maxBufferSize := 10

		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().Ping(mock.Anything).Return(nil)
		offsetStore.EXPECT().GetCurrentOffset(mock.Anything, projectionID).Return(offset, nil)

		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventsStore.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{shardNumber}, nil)
		eventsStore.EXPECT().GetShardEvents(mock.Anything, shardNumber, offset.GetValue(), uint64(maxBufferSize)).
			Return(events, nextOffsetValue.AsTime().UnixMilli(), nil)

		// create an instance of the projection
		runner := newProjectionRunner(projectionName, handler, eventsStore, offsetStore, withPullInterval(time.Millisecond))
		runner.maxBufferSize = maxBufferSize

		// start the projection
		err = runner.Start(ctx)
		require.NoError(t, err)

		// run the projection
		runner.Run(ctx)

		pause.For(time.Second)

		eventsStore.AssertExpectations(t)
		offsetStore.AssertExpectations(t)
		offsetStore.AssertNotCalled(t, "WriteOffset", mock.Anything, mock.AnythingOfType("*egopb.Offset"))

		assert.False(t, runner.running.Load())

		require.NoError(t, runner.Stop())
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

		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)
		nextOffsetValue := timestamppb.New(time.Now().Add(time.Minute))
		events := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          event,

				Timestamp: timestamp.AsTime().Unix(),
				Shard:     shardNumber,
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
	t.Run("with encrypted events decrypted during processing", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)

		journalStore := testkit2.NewEventsStore()
		require.NoError(t, journalStore.Connect(ctx))
		offsetStore := testkit2.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		keyStore := testkit2.NewKeyStore()
		encryptor := encryption.NewAESEncryptor(keyStore)

		handler := projection.NewDiscardHandler()

		// write an encrypted event
		eventAny, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)

		eventBytes, err := proto.Marshal(eventAny)
		require.NoError(t, err)

		ciphertext, keyID, err := encryptor.Encrypt(ctx, persistenceID, eventBytes)
		require.NoError(t, err)

		encryptedEvent := &anypb.Any{TypeUrl: eventAny.GetTypeUrl(), Value: ciphertext}
		timestamp := time.Now().Unix()

		journals := []*egopb.Event{
			{
				PersistenceId:   persistenceID,
				SequenceNumber:  1,
				IsDeleted:       false,
				Event:           encryptedEvent,
				Timestamp:       timestamp,
				Shard:           shardNumber,
				IsEncrypted:     true,
				EncryptionKeyId: keyID,
			},
		}
		require.NoError(t, journalStore.WriteEvents(ctx, journals))

		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore,
			withPullInterval(time.Millisecond),
			withEncryptor(encryptor))

		require.NoError(t, runner.Start(ctx))
		runner.Run(ctx)

		pause.For(time.Second)

		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}
		actual, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)

		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with event adapters during processing", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)

		journalStore := testkit2.NewEventsStore()
		require.NoError(t, journalStore.Connect(ctx))
		offsetStore := testkit2.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		handler := projection.NewDiscardHandler()
		adapter := &noopRunnerAdapter{}

		event, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		timestamp := time.Now().Unix()

		journals := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          event,
				Timestamp:      timestamp,
				Shard:          shardNumber,
			},
		}
		require.NoError(t, journalStore.WriteEvents(ctx, journals))

		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore,
			withPullInterval(time.Millisecond),
			withEventAdapters([]eventadapter.EventAdapter{adapter}))

		require.NoError(t, runner.Start(ctx))
		runner.Run(ctx)

		pause.For(time.Second)

		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}
		actual, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)

		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with metrics during processing", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)

		journalStore := testkit2.NewEventsStore()
		require.NoError(t, journalStore.Connect(ctx))
		offsetStore := testkit2.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		handler := projection.NewDiscardHandler()

		meter := noopmetric.NewMeterProvider().Meter("test")
		m := newMetrics(meter)

		event, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		timestamp := time.Now().Unix()

		journals := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          event,
				Timestamp:      timestamp,
				Shard:          shardNumber,
			},
		}
		require.NoError(t, journalStore.WriteEvents(ctx, journals))

		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore,
			withPullInterval(time.Millisecond),
			withMetrics(m))

		require.NoError(t, runner.Start(ctx))
		runner.Run(ctx)

		pause.For(time.Second)

		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}
		actual, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)

		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with dead letter handler on skip failure", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)

		journalStore := testkit2.NewEventsStore()
		require.NoError(t, journalStore.Connect(ctx))
		offsetStore := testkit2.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		handler := &testHandler1{} // always returns error
		dlh := projection.NewDiscardDeadLetterHandler()

		event, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		timestamp := time.Now().Unix()

		journals := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          event,
				Timestamp:      timestamp,
				Shard:          shardNumber,
			},
		}
		require.NoError(t, journalStore.WriteEvents(ctx, journals))

		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore,
			withPullInterval(time.Millisecond),
			withRecoveryStrategy(projection.NewRecovery(
				projection.WithRecoveryPolicy(projection.Skip))),
			withDeadLetterHandler(dlh))

		require.NoError(t, runner.Start(ctx))
		runner.Run(ctx)

		pause.For(time.Second)

		// with Skip policy, events are skipped and offset is still committed
		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}
		actual, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)

		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with dead letter handler error logging", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)

		journalStore := testkit2.NewEventsStore()
		require.NoError(t, journalStore.Connect(ctx))
		offsetStore := testkit2.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		handler := &testHandler1{} // always returns error
		dlh := &errorDeadLetterHandler{}

		event, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		timestamp := time.Now().Unix()

		journals := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          event,
				Timestamp:      timestamp,
				Shard:          shardNumber,
			},
		}
		require.NoError(t, journalStore.WriteEvents(ctx, journals))

		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore,
			withPullInterval(time.Millisecond),
			withRecoveryStrategy(projection.NewRecovery(
				projection.WithRecoveryPolicy(projection.Skip))),
			withDeadLetterHandler(dlh))

		require.NoError(t, runner.Start(ctx))
		runner.Run(ctx)

		pause.For(time.Second)

		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
	})
	t.Run("with starting offset configured", func(t *testing.T) {
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)

		journalStore := testkit2.NewEventsStore()
		require.NoError(t, journalStore.Connect(ctx))
		offsetStore := testkit2.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		handler := projection.NewDiscardHandler()

		event, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		timestamp := time.Now().Unix()

		journals := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          event,
				Timestamp:      timestamp,
				Shard:          shardNumber,
			},
		}
		require.NoError(t, journalStore.WriteEvents(ctx, journals))

		startOffset := time.Now().Add(-time.Hour)
		runner := newProjectionRunner(projectionName, handler, journalStore, offsetStore,
			withPullInterval(time.Millisecond),
			withStartOffset(startOffset))

		require.NoError(t, runner.Start(ctx))
		runner.Run(ctx)

		pause.For(time.Second)

		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, runner.Stop())
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

		event, err := anypb.New(&testpb.AccountCredited{})
		require.NoError(t, err)
		nextOffsetValue := timestamppb.New(time.Now().Add(time.Minute))
		events := []*egopb.Event{
			{
				PersistenceId:  persistenceID,
				SequenceNumber: 1,
				IsDeleted:      false,
				Event:          event,
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

func (x testHandler1) Handle(_ context.Context, _ string, _ *anypb.Any, _ uint64) error {
	return errors.New("damn")
}

type testHandler2 struct {
	counter *atomic.Int32
}

func (x testHandler2) Handle(_ context.Context, _ string, _ *anypb.Any, revision uint64) error {
	if (int(revision) % 2) == 0 {
		return errors.New("failed underlying")
	}
	x.counter.Inc()
	return nil
}

type testPanicHandler struct{}

var _ projection.Handler = &testPanicHandler{}

func (x testPanicHandler) Handle(_ context.Context, _ string, _ *anypb.Any, _ uint64) error {
	panic("boom")
}

type errorDeadLetterHandler struct{}

var _ projection.DeadLetterHandler = &errorDeadLetterHandler{}

func (x *errorDeadLetterHandler) Handle(_ context.Context, _ string, _ string, _ *anypb.Any, _ uint64, _ error) error {
	return errors.New("dead letter handler failed")
}

type noopRunnerAdapter struct{}

var _ eventadapter.EventAdapter = &noopRunnerAdapter{}

func (a *noopRunnerAdapter) Adapt(event *anypb.Any, _ uint64) (*anypb.Any, error) {
	return event, nil
}
