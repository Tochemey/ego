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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/eventstream"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/pause"
	mocks "github.com/tochemey/ego/v4/mocks/persistence"
	"github.com/tochemey/ego/v4/persistence"
	testpb "github.com/tochemey/ego/v4/test/data/testpb"
	"github.com/tochemey/ego/v4/testkit"
)

func TestSagaStatus_String(t *testing.T) {
	tests := []struct {
		status   SagaStatus
		expected string
	}{
		{SagaRunning, "running"},
		{SagaCompleted, "completed"},
		{SagaCompensating, "compensating"},
		{SagaFailed, "failed"},
		{SagaStatus(99), "unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.status.String())
		})
	}
}

func TestSagaActor(t *testing.T) {
	t.Run("PreStart: missing behavior fails to start", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, sagaID).Return(nil, nil)

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		// Spawn with no behavior dependency
		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("PreStart: events store ping failure", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(assert.AnError)

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{id: sagaID}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("PreStart: GetLatestEvent failure", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, sagaID).Return(nil, assert.AnError)

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{id: sagaID}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("PreStart: ReplayEvents failure", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		latestEvent := &egopb.Event{
			PersistenceId:  sagaID,
			SequenceNumber: 3,
		}

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, sagaID).Return(latestEvent, nil)
		eventStore.EXPECT().ReplayEvents(mock.Anything, sagaID, uint64(1), uint64(3), uint64(3)).Return(nil, assert.AnError)

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{id: sagaID}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("PreStart: UnmarshalNew failure during recovery", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		// An event with an unresolvable type URL
		badEvent := &egopb.Event{
			PersistenceId:  sagaID,
			SequenceNumber: 1,
			Event:          &anypb.Any{TypeUrl: "type.googleapis.com/nonexistent.Type", Value: []byte("bad")},
		}
		latestEvent := &egopb.Event{PersistenceId: sagaID, SequenceNumber: 1}

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, sagaID).Return(latestEvent, nil)
		eventStore.EXPECT().ReplayEvents(mock.Anything, sagaID, uint64(1), uint64(1), uint64(1)).Return([]*egopb.Event{badEvent}, nil)

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{id: sagaID}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("PreStart: ApplyEvent failure during recovery", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventMsg, err := anypb.New(&testpb.AccountCreated{AccountId: sagaID, AccountBalance: 100})
		require.NoError(t, err)
		replayedEvent := &egopb.Event{
			PersistenceId:  sagaID,
			SequenceNumber: 1,
			Event:          eventMsg,
		}
		latestEvent := &egopb.Event{PersistenceId: sagaID, SequenceNumber: 1}

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, sagaID).Return(latestEvent, nil)
		eventStore.EXPECT().ReplayEvents(mock.Anything, sagaID, uint64(1), uint64(1), uint64(1)).Return([]*egopb.Event{replayedEvent}, nil)

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{
			id: sagaID,
			applyEvent: func(_ context.Context, _ Event, state State) (State, error) {
				return nil, assert.AnError
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("PreStart: happy path recovery with prior events", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventMsg, err := anypb.New(&testpb.AccountCreated{AccountId: sagaID, AccountBalance: 100})
		require.NoError(t, err)
		replayedEvent := &egopb.Event{
			PersistenceId:  sagaID,
			SequenceNumber: 2,
			Event:          eventMsg,
		}
		latestEvent := &egopb.Event{PersistenceId: sagaID, SequenceNumber: 2}

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, sagaID).Return(latestEvent, nil)
		eventStore.EXPECT().ReplayEvents(mock.Anything, sagaID, uint64(1), uint64(2), uint64(2)).Return([]*egopb.Event{replayedEvent}, nil)
		// the saga journals its start once it is running; this test only
		// asserts the recovery calls, so the write is allowed but not required
		eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(nil).Maybe()

		stream := eventstream.New()
		defer stream.Close()

		applied := make(chan struct{}, 1)
		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{
			id: sagaID,
			applyEvent: func(_ context.Context, _ Event, state State) (State, error) {
				select {
				case applied <- struct{}{}:
				default:
				}
				return state, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)

		select {
		case <-applied:
		case <-time.After(2 * time.Second):
			t.Fatal("ApplyEvent was not called during recovery")
		}

		eventStore.AssertExpectations(t)
		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("Receive: GetStateCommand returns current state", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{id: sagaID}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, new(egopb.GetStateCommand), 3*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply, ok := reply.(*egopb.CommandReply)
		require.True(t, ok)
		stateReply := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		require.NotNil(t, stateReply)
		assert.EqualValues(t, sagaID, stateReply.StateReply.GetPersistenceId())
		// sequence 1 is the running status the saga journals when it starts
		assert.EqualValues(t, 1, stateReply.StateReply.GetSequenceNumber())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("Receive: PostStart with timeout triggers compensation", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		compensated := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
				select {
				case compensated <- struct{}{}:
				default:
				}
				return nil, nil
			},
		}
		// 200ms timeout so the test runs quickly
		sagaCfg := extensions.NewSagaConfig(200 * time.Millisecond)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)

		select {
		case <-compensated:
		case <-time.After(3 * time.Second):
			t.Fatal("compensation was not triggered after timeout")
		}

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("Receive: sagaTimeoutMsg when not running is no-op", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		compensateCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
				select {
				case compensateCalled <- struct{}{}:
				default:
				}
				return nil, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		// Manually send sagaTimeoutMsg twice: first triggers compensation (status → Compensating),
		// second should be a no-op because status is no longer SagaRunning.
		require.NoError(t, goakt.Tell(ctx, pid, &sagaTimeoutMsg{}))
		pause.For(500 * time.Millisecond)

		// Drain first compensation signal
		select {
		case <-compensateCalled:
		case <-time.After(2 * time.Second):
			t.Fatal("first compensation not triggered")
		}

		// Second sagaTimeoutMsg – must not call Compensate again
		require.NoError(t, goakt.Tell(ctx, pid, &sagaTimeoutMsg{}))
		pause.For(500 * time.Millisecond)

		select {
		case <-compensateCalled:
			t.Fatal("Compensate was called again but saga is no longer running")
		default:
			// correct: no second call
		}

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("Receive: unknown message is unhandled", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{id: sagaID}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		// Send an unknown message type – the actor should call ctx.Unhandled() without panicking.
		require.NoError(t, goakt.Tell(ctx, pid, new(emptypb.Empty)))
		pause.For(500 * time.Millisecond)

		// The actor is still alive
		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: skips non-egopb-Event payloads", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		handleEventCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				handleEventCalled <- struct{}{}
				return &SagaAction{}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		// Publish a non-*egopb.Event payload
		stream.Publish(eventsTopic, new(emptypb.Empty))
		pause.For(500 * time.Millisecond)

		select {
		case <-handleEventCalled:
			t.Fatal("HandleEvent should not be called for non-Event payload")
		default:
			// correct
		}

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: skips own saga events", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		handleEventCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				handleEventCalled <- struct{}{}
				return &SagaAction{}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: sagaID})
		ownEvent := &egopb.Event{
			PersistenceId:  sagaID, // same as saga ID → should be skipped
			SequenceNumber: 1,
			Event:          eventAny,
		}
		topic := eventsTopic
		stream.Publish(topic, ownEvent)
		pause.For(500 * time.Millisecond)

		select {
		case <-handleEventCalled:
			t.Fatal("HandleEvent should not be called for own saga events")
		default:
			// correct
		}

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: skips events when saga is not running", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		// the saga processes events on its own goroutine, so the counter must
		// be atomic for the test goroutine to read it safely
		var callCount atomic.Int32
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				callCount.Add(1)
				return &SagaAction{Complete: true}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		domainEvent := &egopb.Event{
			PersistenceId:  uuid.NewString(),
			SequenceNumber: 1,
			Event:          eventAny,
		}
		topic := eventsTopic

		// First event: triggers Complete → saga status becomes SagaCompleted
		stream.Publish(topic, domainEvent)
		pause.For(500 * time.Millisecond)

		countAfterFirst := callCount.Load()

		// Subsequent events should be ignored
		stream.Publish(topic, domainEvent)
		stream.Publish(topic, domainEvent)
		pause.For(500 * time.Millisecond)

		assert.Equal(t, countAfterFirst, callCount.Load(), "HandleEvent should not be called after saga completes")

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: UnmarshalNew error fails the saga once retries are exhausted", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		handled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				select {
				case handled <- struct{}{}:
				default:
				}
				return &SagaAction{}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		// an event whose type is unknown to this process can never be decoded:
		// the saga retries it and then reports the failure through its status
		badEvent := &egopb.Event{
			PersistenceId:  uuid.NewString(),
			SequenceNumber: 1,
			Event:          &anypb.Any{TypeUrl: "type.googleapis.com/nonexistent.Type", Value: []byte("bad")},
		}
		stream.Publish(eventsTopic, badEvent)

		require.Eventually(t, func() bool {
			reply, askErr := goakt.Ask(ctx, pid, new(egopb.GetSagaStatus), 3*time.Second)
			if askErr != nil {
				return false
			}
			return SagaStatus(reply.(*egopb.SagaStatusReply).GetStatus()) == SagaFailed
		}, 15*time.Second, 250*time.Millisecond)

		select {
		case <-handled:
			t.Fatal("HandleEvent was called with an undecodable event")
		default:
		}

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: HandleEvent error is logged and skipped", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		callCount := 0
		secondHandled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				callCount++
				if callCount == 1 {
					return nil, assert.AnError
				}
				select {
				case secondHandled <- struct{}{}:
				default:
				}
				return &SagaAction{}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}

		stream.Publish(topic, event)
		pause.For(300 * time.Millisecond)
		stream.Publish(topic, event)

		select {
		case <-secondHandled:
		case <-time.After(2 * time.Second):
			t.Fatal("saga did not continue processing after HandleEvent error")
		}

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: HandleEvent returns nil action is no-op", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		handled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				handled <- struct{}{}
				return nil, nil // nil action
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handled:
		case <-time.After(2 * time.Second):
			t.Fatal("HandleEvent was not called")
		}
		// Actor must still be alive
		pause.For(300 * time.Millisecond)
		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: HandleEvent Complete action marks saga completed", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Complete: true}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)
		pause.For(500 * time.Millisecond)

		// Actor is still alive but status is Completed
		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: persistAndApplyEvents ApplyEvent error is logged", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, sagaID).Return(nil, nil)
		eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(nil)

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		handled := make(chan struct{}, 1)
		// the saga processes events on its own goroutine, so the counter must
		// be atomic for the test goroutine to read it safely
		var applyCallCount atomic.Int32
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, event Event, _ State) (*SagaAction, error) {
				handled <- struct{}{}
				return &SagaAction{Events: []Event{event}}, nil
			},
			applyEvent: func(_ context.Context, _ Event, state State) (State, error) {
				applyCallCount.Add(1)
				return nil, assert.AnError
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handled:
		case <-time.After(2 * time.Second):
			t.Fatal("HandleEvent was not called")
		}
		pause.For(300 * time.Millisecond)

		// Actor must still be alive despite the error
		require.True(t, pid.IsRunning())
		assert.Positive(t, applyCallCount.Load())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: persistAndApplyEvents WriteEvents error is logged", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, sagaID).Return(nil, nil)
		eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(assert.AnError)

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		handled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, event Event, _ State) (*SagaAction, error) {
				select {
				case handled <- struct{}{}:
				default:
				}
				return &SagaAction{Events: []Event{event}}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handled:
		case <-time.After(2 * time.Second):
			t.Fatal("HandleEvent was not called")
		}
		pause.For(300 * time.Millisecond)

		require.True(t, pid.IsRunning())
		eventStore.AssertExpectations(t)

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: HandleEvent with events persists state", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		applied := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id:           sagaID,
			initialState: func() State { return &samplepb.Account{} },
			handleEvent: func(_ context.Context, event Event, _ State) (*SagaAction, error) {
				return &SagaAction{Events: []Event{event}}, nil
			},
			applyEvent: func(_ context.Context, _ Event, _ State) (State, error) {
				select {
				case applied <- struct{}{}:
				default:
				}
				return &samplepb.Account{AccountBalance: 100}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-applied:
		case <-time.After(2 * time.Second):
			t.Fatal("ApplyEvent was not called")
		}

		// State should have been updated
		pause.For(300 * time.Millisecond)
		reply, err := goakt.Ask(ctx, pid, new(egopb.GetStateCommand), 3*time.Second)
		require.NoError(t, err)
		commandReply := reply.(*egopb.CommandReply)
		stateReply := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		// sequence 1 is the running status journaled at start, 2 is the saga event
		assert.EqualValues(t, 2, stateReply.StateReply.GetSequenceNumber())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("compensate: behavior failure sets SagaFailed", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Compensate: true}, nil
			},
			compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
				return nil, assert.AnError
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)
		pause.For(500 * time.Millisecond)

		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("compensate: command SendSync failure sets SagaFailed", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Compensate: true}, nil
			},
			compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
				return []SagaCommand{
					{EntityID: "nonexistent-entity", Command: new(emptypb.Empty), Timeout: 500 * time.Millisecond},
				}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)
		pause.For(2 * time.Second)

		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("compensate: successful compensation sets SagaCompleted", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()
		targetID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		// Spawn a target actor that accepts compensation commands
		compensationReply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  targetID,
					SequenceNumber: 1,
					State:          mustAny(t, &samplepb.Account{}),
				},
			},
		}
		_, err = actorSystem.Spawn(ctx, targetID,
			&simpleReplyActor{reply: compensationReply},
			goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(500 * time.Millisecond)

		compensated := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Compensate: true}, nil
			},
			compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
				return []SagaCommand{
					{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 3 * time.Second},
				}, nil
			},
			applyEvent: func(_ context.Context, _ Event, state State) (State, error) {
				select {
				case compensated <- struct{}{}:
				default:
				}
				return state, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)
		pause.For(2 * time.Second)

		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("sendCommand: SendSync error triggers HandleError with compensate", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		handleErrorCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Commands: []SagaCommand{
					{EntityID: "nonexistent-entity", Command: new(emptypb.Empty), Timeout: 500 * time.Millisecond},
				}}, nil
			},
			handleError: func(_ context.Context, _ string, _ error, _ State) (*SagaAction, error) {
				select {
				case handleErrorCalled <- struct{}{}:
				default:
				}
				return &SagaAction{Complete: true}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handleErrorCalled:
		case <-time.After(3 * time.Second):
			t.Fatal("HandleError was not called after SendSync failure")
		}

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("sendCommand: HandleError failure is logged", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		handleErrorCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Commands: []SagaCommand{
					{EntityID: "nonexistent-entity", Command: new(emptypb.Empty), Timeout: 500 * time.Millisecond},
				}}, nil
			},
			handleError: func(_ context.Context, _ string, _ error, _ State) (*SagaAction, error) {
				select {
				case handleErrorCalled <- struct{}{}:
				default:
				}
				return nil, assert.AnError
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handleErrorCalled:
		case <-time.After(3 * time.Second):
			t.Fatal("HandleError was not called")
		}
		pause.For(300 * time.Millisecond)
		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("sendCommand: unexpected reply type is logged", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()
		targetID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		// Target responds with a non-CommandReply message
		_, err = actorSystem.Spawn(ctx, targetID,
			&simpleReplyActor{reply: new(emptypb.Empty)},
			goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(500 * time.Millisecond)

		commandSent := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				select {
				case commandSent <- struct{}{}:
				default:
				}
				return &SagaAction{Commands: []SagaCommand{
					{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 3 * time.Second},
				}}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-commandSent:
		case <-time.After(2 * time.Second):
			t.Fatal("command was not sent")
		}
		pause.For(500 * time.Millisecond)

		// Actor must still be running even though reply was unexpected
		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("sendCommand: error reply triggers HandleError", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()
		targetID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		// Target returns an error reply (parseCommandReply will return error)
		errorReply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_ErrorReply{
				ErrorReply: &egopb.ErrorReply{Message: "entity rejected command"},
			},
		}
		_, err = actorSystem.Spawn(ctx, targetID,
			&simpleReplyActor{reply: errorReply},
			goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(500 * time.Millisecond)

		handleErrorCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Commands: []SagaCommand{
					{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 3 * time.Second},
				}}, nil
			},
			handleError: func(_ context.Context, _ string, _ error, _ State) (*SagaAction, error) {
				select {
				case handleErrorCalled <- struct{}{}:
				default:
				}
				return &SagaAction{Complete: true}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handleErrorCalled:
		case <-time.After(3 * time.Second):
			t.Fatal("HandleError was not called after error reply")
		}

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("sendCommand: error reply HandleError failure is logged", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()
		targetID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		errorReply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_ErrorReply{
				ErrorReply: &egopb.ErrorReply{Message: "entity rejected command"},
			},
		}
		_, err = actorSystem.Spawn(ctx, targetID,
			&simpleReplyActor{reply: errorReply},
			goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(500 * time.Millisecond)

		handleErrorCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Commands: []SagaCommand{
					{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 3 * time.Second},
				}}, nil
			},
			handleError: func(_ context.Context, _ string, _ error, _ State) (*SagaAction, error) {
				select {
				case handleErrorCalled <- struct{}{}:
				default:
				}
				return nil, assert.AnError
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handleErrorCalled:
		case <-time.After(3 * time.Second):
			t.Fatal("HandleError was not called")
		}
		pause.For(300 * time.Millisecond)
		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("sendCommand: HandleResult failure is logged", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()
		targetID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		// Target returns a successful state reply
		successReply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  targetID,
					SequenceNumber: 1,
					State:          mustAny(t, &samplepb.Account{}),
				},
			},
		}
		_, err = actorSystem.Spawn(ctx, targetID,
			&simpleReplyActor{reply: successReply},
			goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(500 * time.Millisecond)

		handleResultCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Commands: []SagaCommand{
					{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 3 * time.Second},
				}}, nil
			},
			handleResult: func(_ context.Context, _ string, _ State, _ State) (*SagaAction, error) {
				select {
				case handleResultCalled <- struct{}{}:
				default:
				}
				return nil, assert.AnError
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handleResultCalled:
		case <-time.After(3 * time.Second):
			t.Fatal("HandleResult was not called")
		}
		pause.For(300 * time.Millisecond)
		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("sendCommand: HandleResult success completes saga", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()
		targetID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		successReply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  targetID,
					SequenceNumber: 1,
					State:          mustAny(t, &samplepb.Account{}),
				},
			},
		}
		_, err = actorSystem.Spawn(ctx, targetID,
			&simpleReplyActor{reply: successReply},
			goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(500 * time.Millisecond)

		handleResultCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Commands: []SagaCommand{
					{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 3 * time.Second},
				}}, nil
			},
			handleResult: func(_ context.Context, _ string, _ State, _ State) (*SagaAction, error) {
				select {
				case handleResultCalled <- struct{}{}:
				default:
				}
				return &SagaAction{Complete: true}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handleResultCalled:
		case <-time.After(3 * time.Second):
			t.Fatal("HandleResult was not called")
		}
		pause.For(300 * time.Millisecond)
		require.True(t, pid.IsRunning())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("sendCommand: default timeout when zero", func(t *testing.T) {
		ctx := context.TODO()
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(stream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		handleErrorCalled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				// Timeout=0 should default to 5s in sendCommand
				return &SagaAction{Commands: []SagaCommand{
					{EntityID: "nonexistent-entity", Command: new(emptypb.Empty), Timeout: 0},
				}}, nil
			},
			handleError: func(_ context.Context, _ string, _ error, _ State) (*SagaAction, error) {
				select {
				case handleErrorCalled <- struct{}{}:
				default:
				}
				return &SagaAction{Complete: true}, nil
			},
		}
		sagaCfg := extensions.NewSagaConfig(0)

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, sagaCfg))
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		topic := eventsTopic
		eventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		event := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny}
		stream.Publish(topic, event)

		select {
		case <-handleErrorCalled:
		case <-time.After(10 * time.Second):
			t.Fatal("HandleError was not called (expected after 5s default timeout)")
		}

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
}

// TestSagaPersistAndApplyEventsWriteFailure asserts that a failed journal write
// leaves the saga state and its sequence counter untouched, so the next
// successful write continues the journal without a gap.
func TestSagaPersistAndApplyEventsWriteFailure(t *testing.T) {
	ctx := context.TODO()
	sagaID := uuid.NewString()

	var (
		writeMutex sync.Mutex
		written    []*egopb.Event
		failWrite  atomic.Bool
	)

	eventStore := new(mocks.EventsStore)
	eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, events []*egopb.Event) error {
		if failWrite.Load() {
			return assert.AnError
		}

		writeMutex.Lock()
		written = append(written, events...)
		writeMutex.Unlock()
		return nil
	})

	behavior := &callbackSagaBehavior{
		id:           sagaID,
		initialState: func() State { return new(samplepb.Account) },
		applyEvent: func(_ context.Context, _ Event, state State) (State, error) {
			account := state.(*samplepb.Account)
			return &samplepb.Account{AccountBalance: account.GetAccountBalance() + 1}, nil
		},
	}

	saga := newSagaActor()
	saga.behavior = behavior
	saga.eventsStore = eventStore
	saga.sagaID = sagaID
	saga.currentState = behavior.InitialState()

	event := &testpb.AccountCreated{AccountId: sagaID}

	failWrite.Store(true)
	require.Error(t, saga.persistAndApplyEvents(ctx, []Event{event}))
	assert.EqualValues(t, 0, saga.eventsCounter)
	assert.True(t, proto.Equal(new(samplepb.Account), saga.currentState))

	failWrite.Store(false)
	require.NoError(t, saga.persistAndApplyEvents(ctx, []Event{event}))
	assert.EqualValues(t, 1, saga.eventsCounter)

	writeMutex.Lock()
	defer writeMutex.Unlock()
	require.Len(t, written, 1)
	assert.EqualValues(t, 1, written[0].GetSequenceNumber())
}

// TestSagaEventsCarryShard asserts that saga events are journaled with the
// saga's own shard instead of the default shard zero.
func TestSagaEventsCarryShard(t *testing.T) {
	const sagaShard = 7

	ctx := context.TODO()
	sagaID := uuid.NewString()

	eventStore := testkit.NewEventsStore()
	require.NoError(t, eventStore.Connect(ctx))
	defer eventStore.Disconnect(ctx) //nolint:errcheck

	behavior := &callbackSagaBehavior{id: sagaID}

	saga := newSagaActor()
	saga.behavior = behavior
	saga.eventsStore = eventStore
	saga.sagaID = sagaID
	saga.currentState = behavior.InitialState()
	saga.shard = sagaShard

	require.NoError(t, saga.persistAndApplyEvents(ctx, []Event{&testpb.AccountCreated{AccountId: sagaID}}))

	latest, err := eventStore.GetLatestEvent(ctx, sagaID)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.EqualValues(t, sagaShard, latest.GetShard())
}

// TestSagaStatusSurvivesRestart asserts that a completed saga comes back
// completed after a restart and stops reacting to new events.
func TestSagaStatusSurvivesRestart(t *testing.T) {
	ctx := context.TODO()
	sagaID := uuid.NewString()

	eventStore := testkit.NewEventsStore()
	require.NoError(t, eventStore.Connect(ctx))
	defer eventStore.Disconnect(ctx) //nolint:errcheck

	stream := eventstream.New()
	defer stream.Close()

	actorSystem, err := goakt.NewActorSystem("TestSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithExtensions(
			extensions.NewEventsStore(eventStore),
			extensions.NewEventsStream(stream),
		),
		goakt.WithActorInitMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))
	pause.For(time.Second)

	handled := make(chan struct{}, 8)
	behavior := &callbackSagaBehavior{
		id: sagaID,
		handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
			handled <- struct{}{}
			return &SagaAction{Complete: true}, nil
		},
	}
	sagaCfg := extensions.NewSagaConfig(0)

	_, err = actorSystem.Spawn(ctx, sagaID, newSagaActor(),
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, sagaCfg))
	require.NoError(t, err)
	pause.For(time.Second)

	eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
	require.NoError(t, err)
	stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

	select {
	case <-handled:
	case <-time.After(3 * time.Second):
		t.Fatal("HandleEvent was not called")
	}

	pause.For(500 * time.Millisecond)

	// restart the saga: it must come back completed
	require.NoError(t, actorSystem.Kill(ctx, sagaID))
	pause.For(500 * time.Millisecond)

	pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, sagaCfg))
	require.NoError(t, err)
	pause.For(time.Second)

	reply, err := goakt.Ask(ctx, pid, new(egopb.GetSagaStatus), 3*time.Second)
	require.NoError(t, err)
	statusReply, ok := reply.(*egopb.SagaStatusReply)
	require.True(t, ok)
	assert.EqualValues(t, SagaCompleted, statusReply.GetStatus())

	// a completed saga must not react to new events
	stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 2, Event: eventAny})
	pause.For(time.Second)

	select {
	case <-handled:
		t.Fatal("a completed saga reacted to a new event")
	default:
	}

	stream.Close()
	require.NoError(t, actorSystem.Stop(ctx))
}

// TestSagaStatusReportsTransitions asserts that a saga status query reports the
// status the saga is actually in.
func TestSagaStatusReportsTransitions(t *testing.T) {
	ctx := context.TODO()

	t.Run("running then completed", func(t *testing.T) {
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem := newSagaTestSystem(t, ctx, eventStore, stream)

		handled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				handled <- struct{}{}
				return &SagaAction{Complete: true}, nil
			},
		}

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, extensions.NewSagaConfig(0)))
		require.NoError(t, err)
		pause.For(time.Second)

		assert.Equal(t, SagaRunning, sagaStatusOf(t, ctx, pid))

		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		require.NoError(t, err)
		stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

		select {
		case <-handled:
		case <-time.After(3 * time.Second):
			t.Fatal("HandleEvent was not called")
		}

		pause.For(500 * time.Millisecond)
		assert.Equal(t, SagaCompleted, sagaStatusOf(t, ctx, pid))

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("failed when a compensation fails", func(t *testing.T) {
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem := newSagaTestSystem(t, ctx, eventStore, stream)

		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Compensate: true}, nil
			},
			compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
				return []SagaCommand{
					{EntityID: "nonexistent-entity", Command: new(emptypb.Empty), Timeout: 500 * time.Millisecond},
				}, nil
			},
		}

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, extensions.NewSagaConfig(0)))
		require.NoError(t, err)
		pause.For(time.Second)

		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		require.NoError(t, err)
		stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

		require.Eventually(t, func() bool {
			return sagaStatusOf(t, ctx, pid) == SagaFailed
		}, 10*time.Second, 200*time.Millisecond)

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
}

// TestSagaTimeoutSurvivesRestart asserts that a restarted saga times out on the
// remainder of its original deadline instead of restarting the clock.
func TestSagaTimeoutSurvivesRestart(t *testing.T) {
	const (
		sagaTimeout   = 4 * time.Second
		elapsedBefore = 3 * time.Second
		remainingMax  = 2500 * time.Millisecond
	)

	ctx := context.TODO()
	sagaID := uuid.NewString()

	eventStore := testkit.NewEventsStore()
	require.NoError(t, eventStore.Connect(ctx))
	defer eventStore.Disconnect(ctx) //nolint:errcheck

	stream := eventstream.New()
	defer stream.Close()

	actorSystem, err := goakt.NewActorSystem("TestSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithExtensions(
			extensions.NewEventsStore(eventStore),
			extensions.NewEventsStream(stream),
		),
		goakt.WithActorInitMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))
	pause.For(time.Second)

	compensated := make(chan struct{}, 1)
	behavior := &callbackSagaBehavior{
		id: sagaID,
		compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
			select {
			case compensated <- struct{}{}:
			default:
			}
			return nil, nil
		},
	}
	sagaCfg := extensions.NewSagaConfig(sagaTimeout)

	_, err = actorSystem.Spawn(ctx, sagaID, newSagaActor(),
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, sagaCfg))
	require.NoError(t, err)

	pause.For(elapsedBefore)
	require.NoError(t, actorSystem.Kill(ctx, sagaID))

	select {
	case <-compensated:
		t.Fatal("the saga timed out before it was restarted")
	default:
	}

	_, err = actorSystem.Spawn(ctx, sagaID, newSagaActor(),
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, sagaCfg))
	require.NoError(t, err)
	restartedAt := time.Now()

	select {
	case <-compensated:
		assert.Less(t, time.Since(restartedAt), remainingMax)
	case <-time.After(2 * sagaTimeout):
		t.Fatal("the restarted saga never timed out")
	}

	stream.Close()
	require.NoError(t, actorSystem.Stop(ctx))
}

// TestSagaTimesOutOnRestartWhenDeadlinePassed asserts that a saga restarted
// after its deadline has already elapsed times out at once instead of waiting
// for a fresh timeout.
func TestSagaTimesOutOnRestartWhenDeadlinePassed(t *testing.T) {
	const (
		sagaTimeout    = 2 * time.Second
		downtime       = 3 * time.Second
		timeoutLatency = time.Second
	)

	ctx := context.TODO()
	sagaID := uuid.NewString()

	eventStore := testkit.NewEventsStore()
	require.NoError(t, eventStore.Connect(ctx))
	defer eventStore.Disconnect(ctx) //nolint:errcheck

	stream := eventstream.New()
	defer stream.Close()

	actorSystem, err := goakt.NewActorSystem("TestSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithExtensions(
			extensions.NewEventsStore(eventStore),
			extensions.NewEventsStream(stream),
		),
		goakt.WithActorInitMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))
	pause.For(time.Second)

	compensated := make(chan struct{}, 1)
	behavior := &callbackSagaBehavior{
		id: sagaID,
		compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
			select {
			case compensated <- struct{}{}:
			default:
			}
			return nil, nil
		},
	}
	sagaCfg := extensions.NewSagaConfig(sagaTimeout)

	_, err = actorSystem.Spawn(ctx, sagaID, newSagaActor(),
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, sagaCfg))
	require.NoError(t, err)

	// stop the saga well before its deadline, then bring it back well after
	require.NoError(t, actorSystem.Kill(ctx, sagaID))
	pause.For(downtime)

	select {
	case <-compensated:
		t.Fatal("the stopped saga timed out on its own")
	default:
	}

	_, err = actorSystem.Spawn(ctx, sagaID, newSagaActor(),
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, sagaCfg))
	require.NoError(t, err)

	select {
	case <-compensated:
	case <-time.After(timeoutLatency):
		t.Fatal("the restarted saga did not time out right away")
	}

	stream.Close()
	require.NoError(t, actorSystem.Stop(ctx))
}

// TestSagaRemainsResponsiveDuringParticipantCall asserts that a slow
// participant does not block the saga mailbox: a status query issued while the
// participant is still working is answered right away.
func TestSagaRemainsResponsiveDuringParticipantCall(t *testing.T) {
	const (
		participantDelay = 2 * time.Second
		statusAskTimeout = time.Second
	)

	ctx := context.TODO()
	sagaID := uuid.NewString()
	targetID := uuid.NewString()

	eventStore := testkit.NewEventsStore()
	require.NoError(t, eventStore.Connect(ctx))
	defer eventStore.Disconnect(ctx) //nolint:errcheck

	stream := eventstream.New()
	defer stream.Close()

	actorSystem, err := goakt.NewActorSystem("TestSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithExtensions(
			extensions.NewEventsStore(eventStore),
			extensions.NewEventsStream(stream),
		),
		goakt.WithActorInitMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))
	pause.For(time.Second)

	successReply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  targetID,
				SequenceNumber: 1,
				State:          mustAny(t, &samplepb.Account{}),
			},
		},
	}
	_, err = actorSystem.Spawn(ctx, targetID,
		&delayedReplyActor{reply: successReply, delay: participantDelay},
		goakt.WithLongLived())
	require.NoError(t, err)
	pause.For(500 * time.Millisecond)

	commandSent := make(chan struct{}, 1)
	behavior := &callbackSagaBehavior{
		id: sagaID,
		handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
			select {
			case commandSent <- struct{}{}:
			default:
			}
			return &SagaAction{Commands: []SagaCommand{
				{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 2 * participantDelay},
			}}, nil
		},
	}

	pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, extensions.NewSagaConfig(0)))
	require.NoError(t, err)
	pause.For(time.Second)

	eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
	require.NoError(t, err)
	stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

	select {
	case <-commandSent:
	case <-time.After(3 * time.Second):
		t.Fatal("the saga did not send the command")
	}

	// the participant is still working: the saga must answer anyway
	reply, err := goakt.Ask(ctx, pid, new(egopb.GetSagaStatus), statusAskTimeout)
	require.NoError(t, err)
	statusReply, ok := reply.(*egopb.SagaStatusReply)
	require.True(t, ok)
	assert.EqualValues(t, SagaRunning, statusReply.GetStatus())

	stream.Close()
	require.NoError(t, actorSystem.Stop(ctx))
}

// delayedReplyActor is a test actor that answers a fixed reply after a delay,
// standing in for a slow saga participant.
type delayedReplyActor struct {
	reply proto.Message
	delay time.Duration
}

var _ goakt.Actor = (*delayedReplyActor)(nil)

// PreStart is a no-op.
func (a *delayedReplyActor) PreStart(_ *goakt.Context) error { return nil }

// PostStop is a no-op.
func (a *delayedReplyActor) PostStop(_ *goakt.Context) error { return nil }

// Receive waits for the configured delay before replying to a command.
func (a *delayedReplyActor) Receive(ctx *goakt.ReceiveContext) {
	if _, ok := ctx.Message().(*goakt.PostStart); ok {
		return
	}

	pause.For(a.delay)
	ctx.Response(a.reply)
}

// TestSagaCompensation covers how a saga drives its compensation round.
func TestSagaCompensation(t *testing.T) {
	ctx := context.TODO()

	t.Run("every participant is compensated even after a failure", func(t *testing.T) {
		sagaID := uuid.NewString()
		missingID := "nonexistent-" + uuid.NewString()
		targetID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem := newSagaTestSystem(t, ctx, eventStore, stream)

		compensated := make(chan struct{}, 1)
		_, err := actorSystem.Spawn(ctx, targetID,
			&notifyingReplyActor{reply: new(emptypb.Empty), received: compensated},
			goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(500 * time.Millisecond)

		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Compensate: true}, nil
			},
			compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
				return []SagaCommand{
					{EntityID: missingID, Command: new(emptypb.Empty), Timeout: 500 * time.Millisecond},
					{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 3 * time.Second},
				}, nil
			},
		}

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, extensions.NewSagaConfig(0)))
		require.NoError(t, err)
		pause.For(time.Second)

		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		require.NoError(t, err)
		stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

		select {
		case <-compensated:
		case <-time.After(5 * time.Second):
			t.Fatal("the second participant was never compensated")
		}

		require.Eventually(t, func() bool {
			return sagaStatusOf(t, ctx, pid) == SagaFailed
		}, 10*time.Second, 200*time.Millisecond)

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("reports compensating while the round is in flight", func(t *testing.T) {
		sagaID := uuid.NewString()
		targetID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem := newSagaTestSystem(t, ctx, eventStore, stream)

		_, err := actorSystem.Spawn(ctx, targetID,
			&delayedReplyActor{reply: new(emptypb.Empty), delay: 2 * time.Second},
			goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(500 * time.Millisecond)

		compensating := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return &SagaAction{Compensate: true}, nil
			},
			compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
				select {
				case compensating <- struct{}{}:
				default:
				}
				return []SagaCommand{
					{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 5 * time.Second},
				}, nil
			},
		}

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, extensions.NewSagaConfig(0)))
		require.NoError(t, err)
		pause.For(time.Second)

		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		require.NoError(t, err)
		stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

		select {
		case <-compensating:
		case <-time.After(3 * time.Second):
			t.Fatal("Compensate was not called")
		}

		assert.Equal(t, SagaCompensating, sagaStatusOf(t, ctx, pid))

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
}

// notifyingReplyActor is a test actor that signals every command it receives
// before answering with a fixed reply.
type notifyingReplyActor struct {
	reply    proto.Message
	received chan struct{}
}

var _ goakt.Actor = (*notifyingReplyActor)(nil)

// PreStart is a no-op.
func (a *notifyingReplyActor) PreStart(_ *goakt.Context) error { return nil }

// PostStop is a no-op.
func (a *notifyingReplyActor) PostStop(_ *goakt.Context) error { return nil }

// Receive signals the received command and replies.
func (a *notifyingReplyActor) Receive(ctx *goakt.ReceiveContext) {
	if _, ok := ctx.Message().(*goakt.PostStart); ok {
		return
	}

	select {
	case a.received <- struct{}{}:
	default:
	}

	ctx.Response(a.reply)
}

// TestSagaRetriesHandlerErrors asserts that a saga retries a failing event
// handler instead of dropping the event, and reports a failure that outlives
// the retries through its status.
func TestSagaRetriesHandlerErrors(t *testing.T) {
	ctx := context.TODO()

	t.Run("a transient handler error is retried", func(t *testing.T) {
		const failures = 2

		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem := newSagaTestSystem(t, ctx, eventStore, stream)

		var attempts atomic.Int32
		applied := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id:           sagaID,
			initialState: func() State { return new(samplepb.Account) },
			handleEvent: func(_ context.Context, event Event, _ State) (*SagaAction, error) {
				if attempts.Add(1) <= failures {
					return nil, assert.AnError
				}
				return &SagaAction{Events: []Event{event}}, nil
			},
			applyEvent: func(_ context.Context, _ Event, state State) (State, error) {
				select {
				case applied <- struct{}{}:
				default:
				}
				return state, nil
			},
		}

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, extensions.NewSagaConfig(0)))
		require.NoError(t, err)
		pause.For(time.Second)

		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		require.NoError(t, err)
		stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

		select {
		case <-applied:
		case <-time.After(10 * time.Second):
			t.Fatal("the event was dropped instead of being retried")
		}

		assert.EqualValues(t, failures+1, attempts.Load())

		reply, err := goakt.Ask(ctx, pid, new(egopb.GetSagaStatus), 3*time.Second)
		require.NoError(t, err)
		assert.EqualValues(t, SagaRunning, reply.(*egopb.SagaStatusReply).GetStatus())

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("an exhausted handler error fails the saga", func(t *testing.T) {
		sagaID := uuid.NewString()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		defer eventStore.Disconnect(ctx) //nolint:errcheck

		stream := eventstream.New()
		defer stream.Close()

		actorSystem := newSagaTestSystem(t, ctx, eventStore, stream)

		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				return nil, assert.AnError
			},
		}

		pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
			goakt.WithLongLived(),
			goakt.WithDependencies(behavior, extensions.NewSagaConfig(0)))
		require.NoError(t, err)
		pause.For(time.Second)

		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		require.NoError(t, err)
		stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

		require.Eventually(t, func() bool {
			reply, askErr := goakt.Ask(ctx, pid, new(egopb.GetSagaStatus), 3*time.Second)
			if askErr != nil {
				return false
			}
			return SagaStatus(reply.(*egopb.SagaStatusReply).GetStatus()) == SagaFailed
		}, 15*time.Second, 250*time.Millisecond)

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
}

// newSagaTestSystem starts an actor system wired with the given events store
// and event stream, ready to host a saga actor.
func newSagaTestSystem(t *testing.T, ctx context.Context, eventStore persistence.EventsStore, stream eventstream.Stream) goakt.ActorSystem {
	t.Helper()

	actorSystem, err := goakt.NewActorSystem("TestSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithExtensions(
			extensions.NewEventsStore(eventStore),
			extensions.NewEventsStream(stream),
		),
		goakt.WithActorInitMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))
	pause.For(time.Second)

	return actorSystem
}

// sagaStatusOf asks the saga behind pid for its current status.
func sagaStatusOf(t *testing.T, ctx context.Context, pid *goakt.PID) SagaStatus {
	t.Helper()

	reply, err := goakt.Ask(ctx, pid, new(egopb.GetSagaStatus), 3*time.Second)
	require.NoError(t, err)
	statusReply, ok := reply.(*egopb.SagaStatusReply)
	require.True(t, ok)

	return SagaStatus(statusReply.GetStatus())
}

// mustAny wraps proto.Message in anypb.Any, failing the test on error.
func mustAny(t *testing.T, msg proto.Message) *anypb.Any {
	t.Helper()
	a, err := anypb.New(msg)
	require.NoError(t, err)
	return a
}

// TestSagaIgnoresLateResultsAfterCompletion asserts that a participant reply
// arriving after the saga has completed is not fed to the behavior, so a
// finished saga cannot be driven into new commands or into compensation.
func TestSagaIgnoresLateResultsAfterCompletion(t *testing.T) {
	const (
		participantDelay = time.Second
		lateResultMargin = 2 * time.Second
	)

	ctx := context.TODO()
	sagaID := uuid.NewString()
	targetID := uuid.NewString()

	eventStore := testkit.NewEventsStore()
	require.NoError(t, eventStore.Connect(ctx))
	defer eventStore.Disconnect(ctx) //nolint:errcheck

	stream := eventstream.New()
	defer stream.Close()

	actorSystem, err := goakt.NewActorSystem("TestSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithExtensions(
			extensions.NewEventsStore(eventStore),
			extensions.NewEventsStream(stream),
		),
		goakt.WithActorInitMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))
	pause.For(time.Second)

	successReply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  targetID,
				SequenceNumber: 1,
				State:          mustAny(t, &samplepb.Account{}),
			},
		},
	}
	_, err = actorSystem.Spawn(ctx, targetID,
		&delayedReplyActor{reply: successReply, delay: participantDelay},
		goakt.WithLongLived())
	require.NoError(t, err)
	pause.For(500 * time.Millisecond)

	resultHandled := make(chan struct{}, 1)
	behavior := &callbackSagaBehavior{
		id: sagaID,
		handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
			return &SagaAction{
				Commands: []SagaCommand{{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 5 * time.Second}},
				Complete: true,
			}, nil
		},
		handleResult: func(_ context.Context, _ string, _ State, _ State) (*SagaAction, error) {
			select {
			case resultHandled <- struct{}{}:
			default:
			}
			return &SagaAction{Compensate: true}, nil
		},
	}

	pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, extensions.NewSagaConfig(0)))
	require.NoError(t, err)
	pause.For(time.Second)

	eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
	require.NoError(t, err)
	stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

	sagaStatusOf := func() SagaStatus {
		reply, askErr := goakt.Ask(ctx, pid, new(egopb.GetSagaStatus), 3*time.Second)
		require.NoError(t, askErr)
		return SagaStatus(reply.(*egopb.SagaStatusReply).GetStatus())
	}

	require.Eventually(t, func() bool {
		return sagaStatusOf() == SagaCompleted
	}, 5*time.Second, 100*time.Millisecond)

	// let the slow participant answer: the reply must be dropped
	pause.For(participantDelay + lateResultMargin)

	select {
	case <-resultHandled:
		t.Fatal("HandleResult was called on a completed saga")
	default:
	}

	assert.Equal(t, SagaCompleted, sagaStatusOf())

	stream.Close()
	require.NoError(t, actorSystem.Stop(ctx))
}

// TestSagaCompensationRejectedByParticipantFails asserts that a participant
// answering a compensation command with an error reply counts as a failed
// compensation, not a successful one.
func TestSagaCompensationRejectedByParticipantFails(t *testing.T) {
	ctx := context.TODO()
	sagaID := uuid.NewString()
	targetID := uuid.NewString()

	eventStore := testkit.NewEventsStore()
	require.NoError(t, eventStore.Connect(ctx))
	defer eventStore.Disconnect(ctx) //nolint:errcheck

	stream := eventstream.New()
	defer stream.Close()

	actorSystem, err := goakt.NewActorSystem("TestSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithExtensions(
			extensions.NewEventsStore(eventStore),
			extensions.NewEventsStream(stream),
		),
		goakt.WithActorInitMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))
	pause.For(time.Second)

	rejection := &egopb.CommandReply{
		Reply: &egopb.CommandReply_ErrorReply{
			ErrorReply: &egopb.ErrorReply{Message: "cannot undo"},
		},
	}
	_, err = actorSystem.Spawn(ctx, targetID, &simpleReplyActor{reply: rejection}, goakt.WithLongLived())
	require.NoError(t, err)
	pause.For(500 * time.Millisecond)

	behavior := &callbackSagaBehavior{
		id: sagaID,
		handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
			return &SagaAction{Compensate: true}, nil
		},
		compensate: func(_ context.Context, _ State) ([]SagaCommand, error) {
			return []SagaCommand{{EntityID: targetID, Command: new(emptypb.Empty), Timeout: 3 * time.Second}}, nil
		},
	}

	pid, err := actorSystem.Spawn(ctx, sagaID, newSagaActor(),
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, extensions.NewSagaConfig(0)))
	require.NoError(t, err)
	pause.For(time.Second)

	eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
	require.NoError(t, err)
	stream.Publish(eventsTopic, &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Event: eventAny})

	require.Eventually(t, func() bool {
		reply, askErr := goakt.Ask(ctx, pid, new(egopb.GetSagaStatus), 3*time.Second)
		if askErr != nil {
			return false
		}
		return SagaStatus(reply.(*egopb.SagaStatusReply).GetStatus()) == SagaFailed
	}, 10*time.Second, 200*time.Millisecond)

	stream.Close()
	require.NoError(t, actorSystem.Stop(ctx))
}

// TestSagaRecordStatusRetriesJournalWrite asserts that a transient store error
// does not lose a status transition: the write is retried and the journal ends
// up holding the status the saga is in.
func TestSagaRecordStatusRetriesJournalWrite(t *testing.T) {
	ctx := context.TODO()
	sagaID := uuid.NewString()

	eventStore := new(mocks.EventsStore)
	eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(assert.AnError).Once()
	eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(nil).Once()

	behavior := &callbackSagaBehavior{id: sagaID}

	saga := newSagaActor()
	saga.behavior = behavior
	saga.eventsStore = eventStore
	saga.sagaID = sagaID
	saga.currentState = behavior.InitialState()
	saga.logger = log.DiscardLogger

	saga.recordStatus(ctx, SagaCompleted)

	assert.Equal(t, SagaCompleted, saga.status)
	assert.EqualValues(t, 1, saga.eventsCounter)
	eventStore.AssertExpectations(t)
}
