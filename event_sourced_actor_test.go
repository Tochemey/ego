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
	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/pause"
	mocks "github.com/tochemey/ego/v4/mocks/persistence"
	testpb "github.com/tochemey/ego/v4/test/data/testpb"
	"github.com/tochemey/ego/v4/testkit"
)

func TestEventSourcedActor(t *testing.T) {
	t.Run("with state reply", func(t *testing.T) {
		ctx := context.TODO()

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err := eventStore.Connect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
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
		actor := newEventSourcedActor()
		// spawn the actor
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
		assert.True(t, proto.Equal(expected, resultingState))

		// disconnect the events store
		err = eventStore.Disconnect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// close the stream
		eventStream.Close()
		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with error reply", func(t *testing.T) {
		ctx := context.TODO()

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err := eventStore.Connect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
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
		persistentActor := newEventSourcedActor()
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

		// disconnect the event store
		require.NoError(t, eventStore.Disconnect(ctx))
		// close the stream
		eventStream.Close()

		pause.For(time.Second)

		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with unhandled command", func(t *testing.T) {
		ctx := context.TODO()

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err := eventStore.Connect(ctx)
		require.NoError(t, err)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
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
		persistentActor := newEventSourcedActor()
		// spawn the actor
		pid, _ := actorSystem.Spawn(ctx, behavior.ID(), persistentActor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NotNil(t, pid)

		pause.For(time.Second)

		command := &testpb.TestSend{}
		// send the command to the actor
		reply, err := goakt.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		errorReply := commandReply.GetReply().(*egopb.CommandReply_ErrorReply)
		assert.Equal(t, "unhandled command", errorReply.ErrorReply.GetMessage())

		// disconnect from the event store
		require.NoError(t, eventStore.Disconnect(ctx))

		// close the stream
		eventStream.Close()
		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with state recovery from event store", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		pause.For(time.Second)

		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err := eventStore.Connect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create an instance of event stream
		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
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
		persistentActor := newEventSourcedActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
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

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		// close the stream
		eventStream.Close()

		pause.For(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with no event to persist", func(t *testing.T) {
		ctx := context.TODO()

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err := eventStore.Connect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create an instance of event stream
		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
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
		actor := newEventSourcedActor()
		// spawn the actor
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
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		// create the expected response
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
		assert.True(t, proto.Equal(expected, resultingState))

		// test no events to persist
		command = new(testpb.TestNoEvent)
		// send a command
		reply, err = goakt.Ask(ctx, pid, command, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		commandReply = reply.(*egopb.CommandReply)

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

		// disconnect from the event store
		assert.NoError(t, eventStore.Disconnect(ctx))
		// close the stream
		eventStream.Close()

		pause.For(time.Second)

		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with unhandled event", func(t *testing.T) {
		ctx := context.TODO()

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err := eventStore.Connect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
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
		actor := newEventSourcedActor()
		// spawn the actor
		pid, _ := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NotNil(t, pid)

		pause.For(time.Second)

		// send the command to the actor
		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
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

		reply, err = goakt.Ask(ctx, pid, new(emptypb.Empty), 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		errorReply := commandReply.GetReply().(*egopb.CommandReply_ErrorReply)
		assert.Equal(t, "unhandled event", errorReply.ErrorReply.GetMessage())

		// disconnect from the event store
		require.NoError(t, eventStore.Disconnect(ctx))

		pause.For(time.Second)

		// close the stream
		eventStream.Close()
		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("With events store ping failed", func(t *testing.T) {
		ctx := context.TODO()

		// create an instance of events stream
		eventStream := eventstream.New()

		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(assert.AnError)

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
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
		actor := newEventSourcedActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithLongLived(), goakt.WithDependencies(behavior))
		require.Error(t, err)
		require.Nil(t, pid)

		// close the stream
		eventStream.Close()
		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("With events store GetLatestEvent failed", func(t *testing.T) {
		ctx := context.TODO()

		// create an instance of events stream
		eventStream := eventstream.New()

		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, persistenceID).Return(nil, assert.AnError)

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
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
		actor := newEventSourcedActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		// close the stream
		eventStream.Close()
		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("With replay events failure during recovery", func(t *testing.T) {
		ctx := context.TODO()

		// create an instance of events stream
		eventStream := eventstream.New()

		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		latestEvent := &egopb.Event{
			PersistenceId:  persistenceID,
			SequenceNumber: 1,
		}

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, persistenceID).Return(latestEvent, nil)
		eventStore.EXPECT().ReplayEvents(mock.Anything, persistenceID, uint64(1), uint64(1), mock.AnythingOfType("uint64")).
			Return(nil, assert.AnError)

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
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
		actor := newEventSourcedActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		// close the stream
		eventStream.Close()
		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with snapshot store recovery", func(t *testing.T) {
		ctx := context.TODO()

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create the snapshot store
		snapshotStore := testkit.NewSnapshotStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the stores
		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))

		pause.For(time.Second)

		// pre-write a snapshot
		stateAny, err := anypb.New(&testpb.Account{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		snapshot := &egopb.Snapshot{
			PersistenceId:  persistenceID,
			SequenceNumber: 1,
			State:          stateAny,
			Timestamp:      time.Now().Unix(),
		}
		require.NoError(t, snapshotStore.WriteSnapshot(ctx, snapshot))

		// pre-write an event after the snapshot
		eventAny, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID, AccountBalance: 50})
		require.NoError(t, err)
		event := &egopb.Event{
			PersistenceId:  persistenceID,
			SequenceNumber: 2,
			Event:          eventAny,
			Timestamp:      time.Now().Unix(),
			Shard:          0,
		}
		require.NoError(t, eventStore.WriteEvents(ctx, []*egopb.Event{event}))

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		// fetch the current state
		reply, err := goakt.Ask(ctx, pid, &egopb.GetStateCommand{}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 150.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()

		pause.For(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with telemetry extension", func(t *testing.T) {
		ctx := context.TODO()

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err := eventStore.Connect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create noop tracer and meter for telemetry
		noopTracer := tracenoop.NewTracerProvider().Tracer("test")
		noopMeter := noop.NewMeterProvider().Meter("test")

		// create an actor system with telemetry extension
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewTelemetryExtension(noopTracer, noopMeter),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
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

		// disconnect the event store
		assert.NoError(t, eventStore.Disconnect(ctx))
		// close the stream
		eventStream.Close()

		pause.For(time.Second)

		// stop the actor system (exercises PostStop metrics decrement)
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with encryption during command processing", func(t *testing.T) {
		ctx := context.TODO()

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err := eventStore.Connect(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create a key store and encryptor
		keyStore := testkit.NewKeyStore()
		encryptor := encryption.NewAESEncryptor(keyStore)

		// create an actor system with encryptor extension
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
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

		// disconnect the event store
		assert.NoError(t, eventStore.Disconnect(ctx))
		// close the stream
		eventStream.Close()

		pause.For(time.Second)

		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with snapshot persistence on interval", func(t *testing.T) {
		ctx := context.TODO()

		// create the stores
		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the stores
		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system with snapshot store
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create entity config with snapshot interval of 1
		entityCfg := &extensions.EntityConfig{
			SnapshotInterval: 1,
		}

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor()
		// spawn the actor with behavior and entity config dependencies
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior, entityCfg), goakt.WithLongLived())
		require.NoError(t, err)
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
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		pause.For(time.Second)

		// verify snapshot was written
		snap, err := snapshotStore.GetLatestSnapshot(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, snap)
		assert.EqualValues(t, 1, snap.GetSequenceNumber())

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()

		pause.For(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with retention policy delete events on snapshot", func(t *testing.T) {
		ctx := context.TODO()

		// create the stores
		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the stores
		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system with snapshot store
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create entity config with snapshot interval of 2 and retention policy
		entityCfg := &extensions.EntityConfig{
			SnapshotInterval:       2,
			HasRetentionPolicy:     true,
			DeleteEventsOnSnapshot: true,
			EventsRetentionCount:   0,
		}

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor()
		// spawn the actor with behavior and entity config dependencies
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior, entityCfg), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		// send first command
		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		// send second command to hit snapshot interval
		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 100}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, state.StateReply.GetSequenceNumber())

		pause.For(time.Second)

		// verify snapshot was written
		snap, err := snapshotStore.GetLatestSnapshot(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, snap)
		assert.EqualValues(t, 2, snap.GetSequenceNumber())

		// verify events were deleted (deleteUpTo = eventsCounter = 2 since EventsRetentionCount is 0)
		latestEvent, err := eventStore.GetLatestEvent(ctx, persistenceID)
		require.NoError(t, err)
		assert.Nil(t, latestEvent)

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()

		pause.For(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with event adapters during recovery", func(t *testing.T) {
		ctx := context.TODO()

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		require.NoError(t, eventStore.Connect(ctx))

		pause.For(time.Second)

		// pre-write an event to the store
		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		event := &egopb.Event{
			PersistenceId:  persistenceID,
			SequenceNumber: 1,
			Event:          eventAny,
			Timestamp:      time.Now().Unix(),
			Shard:          0,
		}
		require.NoError(t, eventStore.WriteEvents(ctx, []*egopb.Event{event}))

		// create an instance of events stream
		eventStream := eventstream.New()

		// create a no-op event adapter that passes events through unchanged
		adapter := &noopEventAdapter{}

		// create an actor system with event adapters extension
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewEventAdapters([]eventadapter.EventAdapter{adapter}),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor()
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		// fetch the current state - should have recovered through the adapter chain
		reply, err := goakt.Ask(ctx, pid, &egopb.GetStateCommand{}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 100,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()

		pause.For(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with snapshot and encryption during recovery", func(t *testing.T) {
		ctx := context.TODO()

		// create the stores
		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()

		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the stores
		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create a key store and encryptor
		keyStore := testkit.NewKeyStore()
		encryptor := encryption.NewAESEncryptor(keyStore)

		// create entity config with snapshot interval of 2 (snapshot at event 2, event 3 has no snapshot)
		entityCfg := &extensions.EntityConfig{
			SnapshotInterval: 2,
		}

		// create an actor system with encryption and snapshot store
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor()
		// spawn the actor with behavior and entity config
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior, entityCfg), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		// send command 1 (event 1, no snapshot yet)
		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		// send command 2 (event 2, snapshot taken at seq 2)
		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 200}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		// send command 3 (event 3, no snapshot - this encrypted event will need replay after snapshot)
		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 100}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		pause.For(time.Second)

		// stop the first actor system
		eventStream.Close()
		err = actorSystem.Stop(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create a new events stream
		eventStream2 := eventstream.New()

		// start a NEW actor system with the same stores and encryption
		actorSystem2, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream2),
				extensions.NewSnapshotStore(snapshotStore),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem2)

		err = actorSystem2.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// spawn the actor again - should recover from encrypted snapshot and events
		behavior2 := NewAccountEventSourcedBehavior(persistenceID)
		actor2 := newEventSourcedActor()
		pid2, err := actorSystem2.Spawn(ctx, behavior2.ID(), actor2, goakt.WithDependencies(behavior2, entityCfg), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid2)

		pause.For(time.Second)

		// fetch the current state - should have recovered
		reply, err = goakt.Ask(ctx, pid2, &egopb.GetStateCommand{}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 3, state.StateReply.GetSequenceNumber())

		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		// 500 + 200 + 100 = 800
		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 800.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream2.Close()

		pause.For(time.Second)

		err = actorSystem2.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with encrypted event replay without snapshot store", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		eventStream := eventstream.New()
		keyStore := testkit.NewKeyStore()
		encryptor := encryption.NewAESEncryptor(keyStore)

		// first actor system: send commands with encryption (no snapshot store)
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)
		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 300.00}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 150}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
		pause.For(time.Second)

		// second actor system: recover from encrypted events (no snapshot)
		eventStream2 := eventstream.New()
		actorSystem2, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream2),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		require.NoError(t, actorSystem2.Start(ctx))
		pause.For(time.Second)

		behavior2 := NewAccountEventSourcedBehavior(persistenceID)
		actor2 := newEventSourcedActor()
		pid2, err := actorSystem2.Spawn(ctx, behavior2.ID(), actor2, goakt.WithDependencies(behavior2), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid2)
		pause.For(time.Second)

		reply, err = goakt.Ask(ctx, pid2, &egopb.GetStateCommand{}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, state.StateReply.GetSequenceNumber())

		resultingState := new(testpb.Account)
		require.NoError(t, state.StateReply.GetState().UnmarshalTo(resultingState))

		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 450.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		assert.NoError(t, eventStore.Disconnect(ctx))
		eventStream2.Close()
		pause.For(time.Second)
		assert.NoError(t, actorSystem2.Stop(ctx))
	})
	t.Run("with retention policy delete snapshots on snapshot", func(t *testing.T) {
		ctx := context.TODO()

		// create the stores
		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the stores
		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))

		pause.For(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create an actor system with snapshot store
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// create entity config with snapshot interval of 2 and delete snapshots retention policy
		entityCfg := &extensions.EntityConfig{
			SnapshotInterval:          2,
			HasRetentionPolicy:        true,
			DeleteSnapshotsOnSnapshot: true,
		}

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor()
		// spawn the actor with behavior and entity config dependencies
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior, entityCfg), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		// send 4 commands: snapshots at events 2 and 4
		// command 1: create account
		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		// command 2: credit (triggers first snapshot at seq 2)
		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 100}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		// command 3: credit
		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 50}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		// command 4: credit (triggers second snapshot at seq 4, should delete snapshot at seq 2)
		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 25}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 4, state.StateReply.GetSequenceNumber())

		pause.For(time.Second)

		// verify latest snapshot exists at seq 4
		snap, err := snapshotStore.GetLatestSnapshot(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, snap)
		assert.EqualValues(t, 4, snap.GetSequenceNumber())

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()

		pause.For(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
}

// noopEventAdapter is a no-op event adapter that passes events through unchanged
type noopEventAdapter struct{}

func (a *noopEventAdapter) Adapt(event *anypb.Any, _ uint64) (*anypb.Any, error) {
	return event, nil
}
