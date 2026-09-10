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
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/pause"
	mocks "github.com/tochemey/ego/v4/mocks/persistence"
	"github.com/tochemey/ego/v4/persistence"
	testpb "github.com/tochemey/ego/v4/test/data/testpb"
	"github.com/tochemey/ego/v4/testkit"
)

const (
	// durableStateWait bounds how long a test waits for a state publication.
	durableStateWait = 5 * time.Second
	// durableStatePollInterval is how often those waits read the stream.
	durableStatePollInterval = 100 * time.Millisecond
)

func TestDurableStateBehavior(t *testing.T) {
	t.Run("with state reply", func(t *testing.T) {
		ctx := context.TODO()

		durableStore := testkit.NewDurableStore()

		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)
		err := durableStore.Connect(ctx)
		require.NoError(t, err)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewDurableStateStore(durableStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		actor := newDurableStateActor()
		pid, _ := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NotNil(t, pid)

		pause.For(time.Second)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}
		// send the command to the actor
		reply, err := goakt.Ask(ctx, pid, command, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		require.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 500.00,
		}
		require.True(t, proto.Equal(expected, resultingState))

		// send another command to credit the balance
		command = &testpb.CreditAccount{
			AccountId: persistenceID,
			Balance:   250,
		}
		reply, err = goakt.Ask(ctx, pid, command, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state = commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState = new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected = &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 750.00,
		}
		require.True(t, proto.Equal(expected, resultingState))

		// stop the actor system
		err = actorSystem.Stop(ctx)
		require.NoError(t, err)

		err = durableStore.Disconnect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)
		eventStream.Close()
	})
	t.Run("with error reply", func(t *testing.T) {
		ctx := context.TODO()

		durableStore := testkit.NewDurableStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountDurableStateBehavior(persistenceID)

		err := durableStore.Connect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewDurableStateStore(durableStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create the persistence actor using the behavior previously created
		persistentActor := newDurableStateActor()
		// spawn the actor
		pid, _ := actorSystem.Spawn(ctx, behavior.ID(), persistentActor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NotNil(t, pid)

		pause.For(time.Second)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}
		// send the command to the actor
		reply, err := goakt.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 500.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// send another command to credit the balance
		command = &testpb.CreditAccount{
			AccountId: "different-id",
			Balance:   250,
		}
		reply, err = goakt.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		errorReply := commandReply.GetReply().(*egopb.CommandReply_ErrorReply)
		assert.Equal(t, "command sent to the wrong entity", errorReply.ErrorReply.GetMessage())

		err = actorSystem.Stop(ctx)
		require.NoError(t, err)

		err = durableStore.Disconnect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)
		eventStream.Close()
	})
	t.Run("with state recovery from state store", func(t *testing.T) {
		ctx := context.TODO()

		durableStore := testkit.NewDurableStore()
		require.NoError(t, durableStore.Connect(ctx))

		pause.For(time.Second)

		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)

		err := durableStore.Connect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewDurableStateStore(durableStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		persistentActor := newDurableStateActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}

		reply, err := goakt.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 500.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// send another command to credit the balance
		command = &testpb.CreditAccount{
			AccountId: persistenceID,
			Balance:   250,
		}
		reply, err = goakt.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state = commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState = new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected = &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 750.00,
		}

		assert.True(t, proto.Equal(expected, resultingState))
		// wait a while
		pause.For(time.Second)

		// restart the actor
		pid, err = actorSystem.ReSpawn(ctx, behavior.ID())
		require.NoError(t, err)

		pause.For(time.Second)

		// fetch the current state
		command = &egopb.GetStateCommand{}
		reply, err = goakt.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		resultingState = new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)
		expected = &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 750.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)

		pause.For(time.Second)

		// free resources
		assert.NoError(t, durableStore.Disconnect(ctx))
		eventStream.Close()
	})
	t.Run("with state recovery from state store failure", func(t *testing.T) {
		ctx := context.TODO()
		pause.For(time.Second)

		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)

		eventStream := eventstream.New()

		durableStore := new(mocks.StateStore)
		durableStore.EXPECT().Ping(mock.Anything).Return(nil)
		durableStore.EXPECT().GetLatestState(mock.Anything, behavior.ID()).Return(nil, assert.AnError)

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewDurableStateStore(durableStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		persistentActor := newDurableStateActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		pause.For(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)

		pause.For(time.Second)
		eventStream.Close()
		durableStore.AssertExpectations(t)
	})
	t.Run("with state recovery from state store with initial parsing failure", func(t *testing.T) {
		ctx := context.TODO()

		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)

		eventStream := eventstream.New()

		latestState := &egopb.DurableState{
			ResultingState: &anypb.Any{
				TypeUrl: "invalid-type-url",
				Value:   []byte("invalid-value"),
			},
		}
		durableStore := new(mocks.StateStore)
		durableStore.EXPECT().Ping(mock.Anything).Return(nil)
		durableStore.EXPECT().GetLatestState(mock.Anything, behavior.ID()).Return(latestState, nil)

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewDurableStateStore(durableStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		persistentActor := newDurableStateActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		pause.For(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)

		pause.For(time.Second)

		eventStream.Close()
		durableStore.AssertExpectations(t)
	})
	t.Run("with telemetry extension", func(t *testing.T) {
		ctx := context.TODO()

		durableStore := testkit.NewDurableStore()
		require.NoError(t, durableStore.Connect(ctx))

		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)

		eventStream := eventstream.New()

		noopTracer := tracenoop.NewTracerProvider().Tracer("test")
		noopMeter := noop.NewMeterProvider().Meter("test")

		// create an actor system with telemetry extension
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewDurableStateStore(durableStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewTelemetryExtension(noopTracer, noopMeter),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		require.NotNil(t, actorSystem)

		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		actor := newDurableStateActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		command := &testpb.CreateAccount{AccountBalance: 500.00}
		reply, err := goakt.Ask(ctx, pid, command, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		require.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		// stop the actor system (exercises PostStop metrics path)
		err = actorSystem.Stop(ctx)
		require.NoError(t, err)

		err = durableStore.Disconnect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)
		eventStream.Close()
	})
	t.Run("with mismatched state types from HandleCommand", func(t *testing.T) {
		ctx := context.TODO()

		durableStore := testkit.NewDurableStore()
		require.NoError(t, durableStore.Connect(ctx))

		persistenceID := uuid.NewString()
		behavior := &badStateDurableStateBehavior{id: persistenceID}

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewDurableStateStore(durableStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		require.NotNil(t, actorSystem)

		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		actor := newDurableStateActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		command := &testpb.CreateAccount{AccountBalance: 500.00}
		reply, err := goakt.Ask(ctx, pid, command, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		errorReply := commandReply.GetReply().(*egopb.CommandReply_ErrorReply)
		assert.Contains(t, errorReply.ErrorReply.GetMessage(), "mismatch state types")

		err = actorSystem.Stop(ctx)
		require.NoError(t, err)

		err = durableStore.Disconnect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)
		eventStream.Close()
	})
	t.Run("with invalid version increment from HandleCommand", func(t *testing.T) {
		ctx := context.TODO()

		durableStore := testkit.NewDurableStore()
		require.NoError(t, durableStore.Connect(ctx))

		persistenceID := uuid.NewString()
		behavior := &badVersionDurableStateBehavior{id: persistenceID}

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewDurableStateStore(durableStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		require.NotNil(t, actorSystem)

		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		actor := newDurableStateActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		command := &testpb.CreateAccount{AccountBalance: 500.00}
		reply, err := goakt.Ask(ctx, pid, command, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		errorReply := commandReply.GetReply().(*egopb.CommandReply_ErrorReply)
		assert.Contains(t, errorReply.ErrorReply.GetMessage(), "received version")

		err = actorSystem.Stop(ctx)
		require.NoError(t, err)

		err = durableStore.Disconnect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)
		eventStream.Close()
	})
}

// badStateDurableStateBehavior returns a different state type from HandleCommand than InitialState
type badStateDurableStateBehavior struct {
	id string
}

var _ DurableStateBehavior = (*badStateDurableStateBehavior)(nil)

func (x *badStateDurableStateBehavior) ID() string {
	return x.id
}

func (x *badStateDurableStateBehavior) InitialState() State {
	return new(testpb.Account)
}

func (x *badStateDurableStateBehavior) HandleCommand(_ context.Context, _ Command, priorVersion uint64, _ State) (State, uint64, error) {
	// return a different protobuf type than InitialState
	return &testpb.AccountCreated{
		AccountId:      x.id,
		AccountBalance: 500.00,
	}, priorVersion + 1, nil
}

func (x *badStateDurableStateBehavior) MarshalBinary() ([]byte, error) {
	return json.Marshal(struct {
		ID string `json:"id"`
	}{ID: x.id})
}

func (x *badStateDurableStateBehavior) UnmarshalBinary(data []byte) error {
	aux := struct {
		ID string `json:"id"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	x.id = aux.ID
	return nil
}

// badVersionDurableStateBehavior returns an invalid version increment from HandleCommand
type badVersionDurableStateBehavior struct {
	id string
}

var _ DurableStateBehavior = (*badVersionDurableStateBehavior)(nil)

func (x *badVersionDurableStateBehavior) ID() string {
	return x.id
}

func (x *badVersionDurableStateBehavior) InitialState() State {
	return new(testpb.Account)
}

func (x *badVersionDurableStateBehavior) HandleCommand(_ context.Context, _ Command, priorVersion uint64, _ State) (State, uint64, error) {
	return &testpb.Account{
		AccountId:      x.id,
		AccountBalance: 500.00,
	}, priorVersion + 5, nil // +5 instead of +1
}

func (x *badVersionDurableStateBehavior) MarshalBinary() ([]byte, error) {
	return json.Marshal(struct {
		ID string `json:"id"`
	}{ID: x.id})
}

func (x *badVersionDurableStateBehavior) UnmarshalBinary(data []byte) error {
	aux := struct {
		ID string `json:"id"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	x.id = aux.ID
	return nil
}

// TestDurableStateWriteFailureKeepsState asserts that a durable-state entity
// does not advance its in-memory state or version when the state store write
// fails, so a later command starts from the state the store actually holds.
func TestDurableStateWriteFailureKeepsState(t *testing.T) {
	ctx := context.TODO()

	persistenceID := uuid.NewString()
	behavior := NewAccountDurableStateBehavior(persistenceID)

	stateStore := new(mocks.StateStore)
	stateStore.EXPECT().Ping(mock.Anything).Return(nil)
	stateStore.EXPECT().GetLatestState(mock.Anything, persistenceID).Return(nil, nil)
	stateStore.EXPECT().WriteState(mock.Anything, mock.Anything).Return(assert.AnError).Once()
	stateStore.EXPECT().WriteState(mock.Anything, mock.Anything).Return(nil)

	eventStream := eventstream.New()

	actorSystem, err := goakt.NewActorSystem("TestActorSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithExtensions(
			extensions.NewDurableStateStore(stateStore),
			extensions.NewEventsStream(eventStream),
		),
		goakt.WithActorInitMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))

	pause.For(time.Second)

	pid, err := actorSystem.Spawn(ctx, behavior.ID(), newDurableStateActor(),
		goakt.WithDependencies(behavior),
		goakt.WithLongLived())
	require.NoError(t, err)
	require.NotNil(t, pid)

	pause.For(time.Second)

	// the write fails: the caller is told so
	reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
	require.NoError(t, err)
	require.IsType(t, new(egopb.CommandReply_ErrorReply), reply.(*egopb.CommandReply).GetReply())

	// the entity still answers from the state the store holds
	reply, err = goakt.Ask(ctx, pid, new(egopb.GetStateCommand), 5*time.Second)
	require.NoError(t, err)
	stateReply := reply.(*egopb.CommandReply).GetReply().(*egopb.CommandReply_StateReply)
	assert.EqualValues(t, 0, stateReply.StateReply.GetSequenceNumber())

	currentState := new(testpb.Account)
	require.NoError(t, stateReply.StateReply.GetState().UnmarshalTo(currentState))
	assert.True(t, proto.Equal(new(testpb.Account), currentState))

	// the next successful command bumps the version by exactly one
	reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 250}, 5*time.Second)
	require.NoError(t, err)
	stateReply = reply.(*egopb.CommandReply).GetReply().(*egopb.CommandReply_StateReply)
	assert.EqualValues(t, 1, stateReply.StateReply.GetSequenceNumber())

	currentState = new(testpb.Account)
	require.NoError(t, stateReply.StateReply.GetState().UnmarshalTo(currentState))
	assert.InDelta(t, 250.00, currentState.GetAccountBalance(), 0)

	require.NoError(t, actorSystem.Stop(ctx))
	pause.For(time.Second)
	eventStream.Close()
}

// TestDurableStateDeletion asserts what happens when a durable-state command
// handler deletes the entity's state by returning egopb.DeletedState.
func TestDurableStateDeletion(t *testing.T) {
	ctx := context.TODO()

	t.Run("the state is deleted, published and the entity starts over", func(t *testing.T) {
		durableStore := testkit.NewDurableStore()
		require.NoError(t, durableStore.Connect(ctx))
		t.Cleanup(func() { _ = durableStore.Disconnect(ctx) })

		eventStream := eventstream.New()
		t.Cleanup(eventStream.Close)

		published := eventStream.AddSubscriber()
		eventStream.Subscribe(published, statesTopic)

		persistenceID := uuid.NewString()
		behavior := &closingAccountBehavior{AccountDurableStateBehavior: NewAccountDurableStateBehavior(persistenceID)}
		actorSystem := newDurableStateTestSystem(t, ctx, durableStore, eventStream)

		pid, err := actorSystem.Spawn(ctx, persistenceID, newDurableStateActor(), goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(time.Second)

		created := durableStateReply(t, ctx, pid, &testpb.CreateAccount{AccountBalance: 500})
		require.EqualValues(t, 1, created.GetSequenceNumber())

		deleted := durableStateReply(t, ctx, pid, new(emptypb.Empty))
		assert.EqualValues(t, 2, deleted.GetSequenceNumber())
		assert.True(t, deleted.GetState().MessageIs(new(egopb.DeletedState)), "the reply does not carry the deletion")

		// the store keeps a tombstone: the deleting version and no state
		stored, err := durableStore.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, stored, "the tombstone of the deletion is missing")
		assert.EqualValues(t, 2, stored.GetVersionNumber())
		assert.Nil(t, stored.GetResultingState(), "the durable state was not deleted")

		// the deletion reaches the state subscribers under the version that deleted
		require.Eventually(t, func() bool {
			for _, message := range drainStream(published) {
				state, ok := message.(*egopb.DurableState)
				if ok && state.GetVersionNumber() == 2 && state.GetResultingState().MessageIs(new(egopb.DeletedState)) {
					return true
				}
			}

			return false
		}, durableStateWait, durableStatePollInterval, "the deletion was not published")

		// the entity continues from its initial state at the deleting version,
		// so the versions keep increasing across the deletion
		recreated := durableStateReply(t, ctx, pid, &testpb.CreateAccount{AccountBalance: 10})
		assert.EqualValues(t, 3, recreated.GetSequenceNumber())

		stored, err = durableStore.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.EqualValues(t, 3, stored.GetVersionNumber())

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("a stop after the deletion writes nothing back and a restart recovers the tombstone", func(t *testing.T) {
		durableStore := testkit.NewDurableStore()
		require.NoError(t, durableStore.Connect(ctx))
		t.Cleanup(func() { _ = durableStore.Disconnect(ctx) })

		eventStream := eventstream.New()
		t.Cleanup(eventStream.Close)

		persistenceID := uuid.NewString()
		behavior := &closingAccountBehavior{AccountDurableStateBehavior: NewAccountDurableStateBehavior(persistenceID)}
		actorSystem := newDurableStateTestSystem(t, ctx, durableStore, eventStream)

		pid, err := actorSystem.Spawn(ctx, persistenceID, newDurableStateActor(), goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(time.Second)

		durableStateReply(t, ctx, pid, &testpb.CreateAccount{AccountBalance: 500})
		durableStateReply(t, ctx, pid, new(emptypb.Empty))

		require.NoError(t, actorSystem.Kill(ctx, persistenceID))
		pause.For(500 * time.Millisecond)

		stored, err := durableStore.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Nil(t, stored.GetResultingState(), "stopping the entity wrote its state back after the deletion")

		pid, err = actorSystem.Spawn(ctx, persistenceID, newDurableStateActor(), goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(time.Second)

		// the tombstone recovers as the initial state at the deleting version
		recovered := durableStateReply(t, ctx, pid, new(egopb.GetStateCommand))
		assert.EqualValues(t, 2, recovered.GetSequenceNumber(), "the restarted entity did not recover the deleting version")
		assert.True(t, recovered.GetState().MessageIs(new(testpb.Account)))

		// a stop of the recovered entity writes nothing back either
		require.NoError(t, actorSystem.Kill(ctx, persistenceID))
		pause.For(500 * time.Millisecond)

		stored, err = durableStore.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Nil(t, stored.GetResultingState(), "stopping the recovered entity wrote a state over the tombstone")

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("a deletion whose version does not follow the current one is rejected", func(t *testing.T) {
		durableStore := testkit.NewDurableStore()
		require.NoError(t, durableStore.Connect(ctx))
		t.Cleanup(func() { _ = durableStore.Disconnect(ctx) })

		eventStream := eventstream.New()
		t.Cleanup(eventStream.Close)

		persistenceID := uuid.NewString()
		behavior := &closingAccountBehavior{AccountDurableStateBehavior: NewAccountDurableStateBehavior(persistenceID), versionSkip: 1}
		actorSystem := newDurableStateTestSystem(t, ctx, durableStore, eventStream)

		pid, err := actorSystem.Spawn(ctx, persistenceID, newDurableStateActor(), goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(time.Second)

		durableStateReply(t, ctx, pid, &testpb.CreateAccount{AccountBalance: 500})

		reply, err := goakt.Ask(ctx, pid, new(emptypb.Empty), 5*time.Second)
		require.NoError(t, err)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), reply.(*egopb.CommandReply).GetReply())

		stored, err := durableStore.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, stored, "a rejected deletion removed the state")
		assert.EqualValues(t, 1, stored.GetVersionNumber())

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("a store failure keeps the state and the version", func(t *testing.T) {
		persistenceID := uuid.NewString()

		stateStore := new(mocks.StateStore)
		stateStore.On("Ping", mock.Anything).Return(nil)
		stateStore.On("GetLatestState", mock.Anything, persistenceID).Return(nil, nil)
		stateStore.On("WriteState", mock.Anything, mock.Anything).Return(nil)
		stateStore.On("DeleteState", mock.Anything, persistenceID, mock.Anything).Return(assert.AnError)

		eventStream := eventstream.New()
		t.Cleanup(eventStream.Close)

		behavior := &closingAccountBehavior{AccountDurableStateBehavior: NewAccountDurableStateBehavior(persistenceID)}
		actorSystem := newDurableStateTestSystem(t, ctx, stateStore, eventStream)

		pid, err := actorSystem.Spawn(ctx, persistenceID, newDurableStateActor(), goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		pause.For(time.Second)

		durableStateReply(t, ctx, pid, &testpb.CreateAccount{AccountBalance: 500})

		reply, err := goakt.Ask(ctx, pid, new(emptypb.Empty), 5*time.Second)
		require.NoError(t, err)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), reply.(*egopb.CommandReply).GetReply())

		current := durableStateReply(t, ctx, pid, new(egopb.GetStateCommand))
		assert.EqualValues(t, 1, current.GetSequenceNumber())
		assert.True(t, current.GetState().MessageIs(new(testpb.Account)))

		require.NoError(t, actorSystem.Stop(ctx))
	})
}

// closingAccountBehavior is the account behavior with one more command: an
// empty message closes the account by deleting its durable state.
type closingAccountBehavior struct {
	*AccountDurableStateBehavior
	// versionSkip is added to the version a deletion would normally carry, so
	// a test can hand the entity a version that does not follow the current one.
	versionSkip uint64
}

// HandleCommand deletes the durable state on an empty command and delegates
// every other command to the account behavior.
func (x *closingAccountBehavior) HandleCommand(ctx context.Context, command Command, priorVersion uint64, priorState State) (State, uint64, error) {
	if _, ok := command.(*emptypb.Empty); ok {
		return new(egopb.DeletedState), priorVersion + 1 + x.versionSkip, nil
	}

	return x.AccountDurableStateBehavior.HandleCommand(ctx, command, priorVersion, priorState)
}

// newDurableStateTestSystem starts an actor system wired with the given state
// store and event stream, ready to host a durable-state actor.
func newDurableStateTestSystem(t *testing.T, ctx context.Context, stateStore persistence.StateStore, eventStream eventstream.Stream) goakt.ActorSystem {
	t.Helper()

	actorSystem, err := goakt.NewActorSystem("TestActorSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithExtensions(
			extensions.NewDurableStateStore(stateStore),
			extensions.NewEventsStream(eventStream),
		),
		goakt.WithActorInitMaxRetries(3))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))
	pause.For(time.Second)

	return actorSystem
}

// durableStateReply sends a command to a durable-state actor and returns the
// state reply it answers with, failing the test on any other reply.
func durableStateReply(t *testing.T, ctx context.Context, pid *goakt.PID, command proto.Message) *egopb.StateReply {
	t.Helper()

	reply, err := goakt.Ask(ctx, pid, command, 5*time.Second)
	require.NoError(t, err)

	commandReply, ok := reply.(*egopb.CommandReply)
	require.True(t, ok, "unexpected reply %T", reply)

	stateReply, ok := commandReply.GetReply().(*egopb.CommandReply_StateReply)
	require.True(t, ok, "the entity answered %v", commandReply.GetReply())

	return stateReply.StateReply
}

// drainStream returns the payloads a subscriber has received so far.
func drainStream(subscriber eventstream.Subscriber) []any {
	var payloads []any
	for message := range subscriber.Iterator() {
		payloads = append(payloads, message.Payload())
	}

	return payloads
}
