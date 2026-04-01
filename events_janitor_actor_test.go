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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/pause"
	mocks "github.com/tochemey/ego/v4/mocks/persistence"
)

func TestEventsJanitorActor(t *testing.T) {
	t.Run("deletes events on snapshot with no retention count", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe().Maybe()
		eventStore.EXPECT().DeleteEvents(mock.Anything, "entity-1", uint64(10)).Return(nil)

		actorSystem, err := goakt.NewActorSystem("TestRetentionSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "retention-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		err = goakt.Tell(ctx, pid, &applyRetentionRequest{
			persistenceID:          "entity-1",
			eventsCounter:          10,
			snapshotInterval:       5,
			deleteEventsOnSnapshot: true,
		})
		require.NoError(t, err)

		pause.For(time.Second)

		assert.True(t, pid.IsRunning())
		eventStore.AssertExpectations(t)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("deletes events with retention count", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()

		// eventsCounter=10, retentionCount=3 => deleteUpTo = 10-3 = 7
		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe().Maybe()
		eventStore.EXPECT().DeleteEvents(mock.Anything, "entity-1", uint64(7)).Return(nil)

		actorSystem, err := goakt.NewActorSystem("TestRetentionSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "retention-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		err = goakt.Tell(ctx, pid, &applyRetentionRequest{
			persistenceID:          "entity-1",
			eventsCounter:          10,
			snapshotInterval:       5,
			deleteEventsOnSnapshot: true,
			eventsRetentionCount:   3,
		})
		require.NoError(t, err)

		pause.For(time.Second)

		assert.True(t, pid.IsRunning())
		eventStore.AssertExpectations(t)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("skips event deletion when retention count exceeds counter", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()

		// eventsCounter=2, retentionCount=5 => deleteUpTo=0, no delete call
		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()

		actorSystem, err := goakt.NewActorSystem("TestRetentionSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "retention-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		err = goakt.Tell(ctx, pid, &applyRetentionRequest{
			persistenceID:          "entity-1",
			eventsCounter:          2,
			snapshotInterval:       5,
			deleteEventsOnSnapshot: true,
			eventsRetentionCount:   5,
		})
		require.NoError(t, err)

		pause.For(time.Second)

		assert.True(t, pid.IsRunning())
		eventStore.AssertNotCalled(t, "DeleteEvents", mock.Anything, mock.Anything, mock.Anything)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("deletes snapshots on snapshot", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()

		// eventsCounter=10, snapshotInterval=5 => previousSnapshotSeqNr = 10-5 = 5
		snapshotStore := new(mocks.SnapshotStore)
		snapshotStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		snapshotStore.EXPECT().DeleteSnapshots(mock.Anything, "entity-1", uint64(5)).Return(nil)

		actorSystem, err := goakt.NewActorSystem("TestRetentionSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "retention-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		err = goakt.Tell(ctx, pid, &applyRetentionRequest{
			persistenceID:             "entity-1",
			eventsCounter:             10,
			snapshotInterval:          5,
			deleteSnapshotsOnSnapshot: true,
		})
		require.NoError(t, err)

		pause.For(time.Second)

		assert.True(t, pid.IsRunning())
		snapshotStore.AssertExpectations(t)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("skips snapshot deletion when counter does not exceed interval", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()

		snapshotStore := new(mocks.SnapshotStore)
		snapshotStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()

		actorSystem, err := goakt.NewActorSystem("TestRetentionSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "retention-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		// eventsCounter=5, snapshotInterval=5 => 5 > 5 is false, skip
		err = goakt.Tell(ctx, pid, &applyRetentionRequest{
			persistenceID:             "entity-1",
			eventsCounter:             5,
			snapshotInterval:          5,
			deleteSnapshotsOnSnapshot: true,
		})
		require.NoError(t, err)

		pause.For(time.Second)

		assert.True(t, pid.IsRunning())
		snapshotStore.AssertNotCalled(t, "DeleteSnapshots", mock.Anything, mock.Anything, mock.Anything)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("logs error and continues when event deletion fails after retries", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		eventStore.EXPECT().DeleteEvents(mock.Anything, "entity-1", uint64(10)).Return(assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestRetentionSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "retention-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		err = goakt.Tell(ctx, pid, &applyRetentionRequest{
			persistenceID:          "entity-1",
			eventsCounter:          10,
			snapshotInterval:       5,
			deleteEventsOnSnapshot: true,
		})
		require.NoError(t, err)

		pause.For(3 * time.Second)

		assert.True(t, pid.IsRunning())

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("logs error and continues when snapshot deletion fails after retries", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()

		snapshotStore := new(mocks.SnapshotStore)
		snapshotStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		snapshotStore.EXPECT().DeleteSnapshots(mock.Anything, "entity-1", uint64(5)).Return(assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestRetentionSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "retention-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		err = goakt.Tell(ctx, pid, &applyRetentionRequest{
			persistenceID:             "entity-1",
			eventsCounter:             10,
			snapshotInterval:          5,
			deleteSnapshotsOnSnapshot: true,
		})
		require.NoError(t, err)

		pause.For(3 * time.Second)

		assert.True(t, pid.IsRunning())

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("handles both event and snapshot deletion together", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe().Maybe()
		eventStore.EXPECT().DeleteEvents(mock.Anything, "entity-1", uint64(10)).Return(nil)

		snapshotStore := new(mocks.SnapshotStore)
		snapshotStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		snapshotStore.EXPECT().DeleteSnapshots(mock.Anything, "entity-1", uint64(5)).Return(nil)

		actorSystem, err := goakt.NewActorSystem("TestRetentionSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "retention-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		err = goakt.Tell(ctx, pid, &applyRetentionRequest{
			persistenceID:             "entity-1",
			eventsCounter:             10,
			snapshotInterval:          5,
			deleteEventsOnSnapshot:    true,
			deleteSnapshotsOnSnapshot: true,
		})
		require.NoError(t, err)

		pause.For(time.Second)

		assert.True(t, pid.IsRunning())
		eventStore.AssertExpectations(t)
		snapshotStore.AssertExpectations(t)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("marks unhandled messages as unhandled", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()

		actorSystem, err := goakt.NewActorSystem("TestRetentionSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "retention-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &egopb.NoReply{}, 5*time.Second)
		require.Error(t, err)
		assert.Nil(t, reply)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
}
