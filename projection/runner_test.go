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
	"github.com/tochemey/ego/eventstore/memory"
	memoffsetstore "github.com/tochemey/ego/offsetstore/memory"
	testpb "github.com/tochemey/ego/test/data/pb/v1"
	"github.com/tochemey/goakt/log"
	"go.uber.org/atomic"
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
		shardNumber := uint64(9)
		logger := log.DefaultLogger

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
		handler := NewDiscardHandler(logger)

		// create an instance of the projection
		projection := NewRunner(projectionName, handler, journalStore, offsetStore, WithRefreshInterval(time.Millisecond))
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
				Shard:          shardNumber,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(time.Second)

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
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, projection.Stop(ctx))
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

		projection := NewRunner(projectionName, handler, journalStore, offsetStore, WithRefreshInterval(time.Millisecond))
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
		require.True(t, projection.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(time.Second)

		// here due to the default recovery strategy the projection is stopped
		require.False(t, projection.started.Load())
		// free resources
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, projection.Stop(ctx))
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

		projection := NewRunner(projectionName, handler, journalStore, offsetStore,
			WithRefreshInterval(time.Millisecond),
			WithRecoveryStrategy(NewRecovery(
				WithRecoveryPolicy(RetryAndFail),
				WithRetries(2),
				WithRetryDelay(100*time.Millisecond))))

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
		require.True(t, projection.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(1 * time.Second)

		// let us grab the current offset
		require.False(t, projection.started.Load())

		// free resources
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, projection.Stop(ctx))
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

		projection := NewRunner(projectionName, handler, journalStore, offsetStore,
			WithRefreshInterval(time.Millisecond),
			WithRecoveryStrategy(NewRecovery(
				WithRecoveryPolicy(Skip),
				WithRetries(2),
				WithRetryDelay(100*time.Millisecond))))
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
				Shard:          shard,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(time.Second)

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
		assert.NoError(t, projection.Stop(ctx))
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

		projection := NewRunner(projectionName, handler, journalStore, offsetStore,
			WithRefreshInterval(time.Millisecond),
			WithRecoveryStrategy(NewRecovery(
				WithRecoveryPolicy(RetryAndSkip),
				WithRetries(2),
				WithRetryDelay(100*time.Millisecond))))
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
				Shard:          shard,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.started.Load())

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(time.Second)

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
		assert.NoError(t, projection.Stop(ctx))
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
