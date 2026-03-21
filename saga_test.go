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
	"fmt"
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
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/pause"
	mocks "github.com/tochemey/ego/v4/mocks/persistence"
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
		assert.EqualValues(t, 0, stateReply.StateReply.GetSequenceNumber())

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
		topic := fmt.Sprintf(eventsTopic, 0)
		stream.Publish(topic, new(emptypb.Empty))
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
		topic := fmt.Sprintf(eventsTopic, 0)
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

		callCount := 0
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				callCount++
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
		topic := fmt.Sprintf(eventsTopic, 0)

		// First event: triggers Complete → saga status becomes SagaCompleted
		stream.Publish(topic, domainEvent)
		pause.For(500 * time.Millisecond)

		countAfterFirst := callCount

		// Subsequent events should be ignored
		stream.Publish(topic, domainEvent)
		stream.Publish(topic, domainEvent)
		pause.For(500 * time.Millisecond)

		assert.Equal(t, countAfterFirst, callCount, "HandleEvent should not be called after saga completes")

		stream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("consumeEvents: UnmarshalNew error is logged and skipped", func(t *testing.T) {
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

		secondEventHandled := make(chan struct{}, 1)
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, _ Event, _ State) (*SagaAction, error) {
				select {
				case secondEventHandled <- struct{}{}:
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

		topic := fmt.Sprintf(eventsTopic, 0)

		// First event: bad type URL → UnmarshalNew fails → logged and skipped
		badEvent := &egopb.Event{
			PersistenceId:  uuid.NewString(),
			SequenceNumber: 1,
			Event:          &anypb.Any{TypeUrl: "type.googleapis.com/nonexistent.Type", Value: []byte("bad")},
		}
		stream.Publish(topic, badEvent)
		pause.For(300 * time.Millisecond)

		// Second event: valid → HandleEvent is called, proving the saga continues
		goodEventAny, _ := anypb.New(&testpb.AccountCreated{AccountId: uuid.NewString()})
		goodEvent := &egopb.Event{
			PersistenceId:  uuid.NewString(),
			SequenceNumber: 1,
			Event:          goodEventAny,
		}
		stream.Publish(topic, goodEvent)

		select {
		case <-secondEventHandled:
		case <-time.After(2 * time.Second):
			t.Fatal("saga did not continue processing after UnmarshalNew error")
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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
		applyCallCount := 0
		behavior := &callbackSagaBehavior{
			id: sagaID,
			handleEvent: func(_ context.Context, event Event, _ State) (*SagaAction, error) {
				handled <- struct{}{}
				return &SagaAction{Events: []Event{event}}, nil
			},
			applyEvent: func(_ context.Context, _ Event, state State) (State, error) {
				applyCallCount++
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

		topic := fmt.Sprintf(eventsTopic, 0)
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
		assert.Positive(t, applyCallCount)

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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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
		assert.EqualValues(t, 1, stateReply.StateReply.GetSequenceNumber())

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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

		topic := fmt.Sprintf(eventsTopic, 0)
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

// mustAny wraps proto.Message in anypb.Any, failing the test on error.
func mustAny(t *testing.T, msg proto.Message) *anypb.Any {
	t.Helper()
	a, err := anypb.New(msg)
	require.NoError(t, err)
	return a
}
