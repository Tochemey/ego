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

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)

		var counter atomic.Int32
		// set up the projection
		// create a handler that return successfully
		handler := func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, sequenceNumber uint64) error {
			counter.Inc()
			return nil
		}

		// create an instance of the projection
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
				Shard:          shardNumber,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.isStarted.Load())

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
		assert.EqualValues(t, journals[9].GetTimestamp(), actual.GetCurrentOffset())

		assert.EqualValues(t, 10, counter.Load())

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
		journalStore := memory.NewEventsStore()
		assert.NotNil(t, journalStore)

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
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
		journalStore := memory.NewEventsStore()
		assert.NotNil(t, journalStore)

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)

		// set up the projection
		// create a handler that return successfully
		handler := func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, sequenceNumber uint64) error {
			return errors.New("damn")
		}

		projection := New(projectionName, handler, journalStore, offsetStore, NewRecovery(
			WithRecoveryPolicy(RetryAndFail),
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
		shard := uint64(8)
		logger := log.DefaultLogger

		// set up the event store
		journalStore := memory.NewEventsStore()
		assert.NotNil(t, journalStore)

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)

		// set up the projection
		var counter atomic.Int32
		// create a handler that return successfully
		handler := func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, sequenceNumber uint64) error {
			if (int(sequenceNumber) % 2) == 0 {
				return errors.New("failed handler")
			}
			counter.Inc()
			return nil
		}

		projection := New(projectionName, handler, journalStore, offsetStore, NewRecovery(
			WithRecoveryPolicy(Skip),
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
				Shard:          shard,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.isStarted.Load())

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
		assert.EqualValues(t, 5, counter.Load())

		// free resource
		assert.NoError(t, projection.Stop(ctx))
	})
	t.Run("with failed handler and skip retry strategy", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shard := uint64(7)
		logger := log.DefaultLogger

		// set up the event store
		journalStore := memory.NewEventsStore()
		assert.NotNil(t, journalStore)

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)

		// set up the projection
		var counter atomic.Int32
		// create a handler that return successfully
		handler := func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, sequenceNumber uint64) error {
			if (int(sequenceNumber) % 2) == 0 {
				return errors.New("failed handler")
			}
			counter.Inc()
			return nil
		}

		projection := New(projectionName, handler, journalStore, offsetStore, NewRecovery(
			WithRecoveryPolicy(RetryAndSkip),
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
				Shard:          shard,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))
		require.True(t, projection.isStarted.Load())

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
		assert.EqualValues(t, 5, counter.Load())

		// free resource
		assert.NoError(t, projection.Stop(ctx))
	})
}
