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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/log"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/internal/extensions"
	"github.com/tochemey/ego/v3/internal/lib"
	"github.com/tochemey/ego/v3/projection"
	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
	"github.com/tochemey/ego/v3/testkit"
)

func TestProjection(t *testing.T) {
	t.Run("With happy path", func(t *testing.T) {
		ctx := context.TODO()
		logger := log.DiscardLogger

		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)

		// set up the event store
		journalStore := testkit.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := testkit.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		handler := projection.NewDiscardHandler()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(logger),
			goakt.WithExtensions(
				extensions.NewEventsStore(journalStore),
				extensions.NewOffsetStore(offsetStore),
				extensions.NewProjectionExtension(handler, 500, ZeroTime, ZeroTime, time.Second, projection.NewRecovery())),
			goakt.WithActorInitMaxRetries(3))

		require.NoError(t, err)
		require.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create the actor
		actor := NewProjectionActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, projectionName, actor, goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

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
				Shard:          shardNumber,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))

		// wait for the data to be persisted by the database since this an eventual consistency case
		lib.Pause(time.Second)

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
		require.NoError(t, journalStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("With unhandled message result in deadletter", func(t *testing.T) {
		ctx := context.TODO()
		logger := log.DiscardLogger

		projectionName := "db-writer"

		// set up the event store
		journalStore := testkit.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := testkit.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Connect(ctx))

		handler := projection.NewDiscardHandler()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(logger),
			goakt.WithExtensions(
				extensions.NewEventsStore(journalStore),
				extensions.NewOffsetStore(offsetStore),
				extensions.NewProjectionExtension(handler, 500, time.Time{}, time.Time{}, time.Second, projection.NewRecovery())),
			goakt.WithActorInitMaxRetries(3))

		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create the actor
		actor := NewProjectionActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, projectionName, actor, goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

		message := &testpb.CreateAccount{}
		// send a message to the actor
		err = goakt.Tell(ctx, pid, message)
		require.NoError(t, err)

		lib.Pause(time.Second)
		metric := pid.Metric(ctx)
		require.EqualValues(t, 1, metric.DeadlettersCount())

		// free resources
		require.NoError(t, journalStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, actorSystem.Stop(ctx))
	})
}
