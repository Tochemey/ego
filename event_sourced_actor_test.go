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
	mockencryption "github.com/tochemey/ego/v4/mocks/encryption"
	mockadapter "github.com/tochemey/ego/v4/mocks/eventadapter"
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

func TestEventSourcedActorErrorPaths(t *testing.T) {
	t.Run("with missing behavior fails to start", func(t *testing.T) {
		ctx := context.TODO()

		eventStream := eventstream.New()
		persistenceID := uuid.NewString()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, persistenceID).Return(nil, nil)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		// spawn with no behavior dependency
		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, persistenceID, actor, goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with snapshot store GetLatestSnapshot failure during recovery", func(t *testing.T) {
		ctx := context.TODO()

		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)
		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)

		snapshotStore := new(mocks.SnapshotStore)
		snapshotStore.EXPECT().Ping(mock.Anything).Return(nil)
		snapshotStore.EXPECT().GetLatestSnapshot(mock.Anything, persistenceID).Return(nil, assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
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

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with snapshot decryption failure during recovery", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))
		pause.For(time.Second)

		// write an "encrypted" snapshot with dummy ciphertext
		stateAny, err := anypb.New(&testpb.Account{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		encryptedState := &anypb.Any{TypeUrl: stateAny.GetTypeUrl(), Value: []byte("fake-ciphertext")}
		snapshot := &egopb.Snapshot{
			PersistenceId:   persistenceID,
			SequenceNumber:  1,
			State:           encryptedState,
			Timestamp:       time.Now().Unix(),
			IsEncrypted:     true,
			EncryptionKeyId: "key-1",
		}
		require.NoError(t, snapshotStore.WriteSnapshot(ctx, snapshot))

		eventStream := eventstream.New()

		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Decrypt(mock.Anything, persistenceID, []byte("fake-ciphertext"), "key-1").Return(nil, assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with snapshot unmarshal failure after decryption during recovery", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))
		pause.For(time.Second)

		stateAny, err := anypb.New(&testpb.Account{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		encryptedState := &anypb.Any{TypeUrl: stateAny.GetTypeUrl(), Value: []byte("fake-ciphertext")}
		snapshot := &egopb.Snapshot{
			PersistenceId:   persistenceID,
			SequenceNumber:  1,
			State:           encryptedState,
			Timestamp:       time.Now().Unix(),
			IsEncrypted:     true,
			EncryptionKeyId: "key-1",
		}
		require.NoError(t, snapshotStore.WriteSnapshot(ctx, snapshot))

		eventStream := eventstream.New()

		// return non-proto garbage bytes so proto.Unmarshal fails
		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Decrypt(mock.Anything, persistenceID, []byte("fake-ciphertext"), "key-1").Return([]byte("not-valid-proto"), nil)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with snapshot state type mismatch unmarshal failure during recovery", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))
		pause.For(time.Second)

		// write snapshot with incompatible state type (AccountCredited instead of Account)
		wrongState, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		snapshot := &egopb.Snapshot{
			PersistenceId:  persistenceID,
			SequenceNumber: 1,
			State:          wrongState,
			Timestamp:      time.Now().Unix(),
		}
		require.NoError(t, snapshotStore.WriteSnapshot(ctx, snapshot))

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
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

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with event decryption failure during recovery", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		// write an "encrypted" event with dummy ciphertext
		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		encryptedEvent := &anypb.Any{TypeUrl: eventAny.GetTypeUrl(), Value: []byte("fake-cipher")}
		event := &egopb.Event{
			PersistenceId:   persistenceID,
			SequenceNumber:  1,
			Event:           encryptedEvent,
			Timestamp:       time.Now().Unix(),
			IsEncrypted:     true,
			EncryptionKeyId: "key-1",
		}
		require.NoError(t, eventStore.WriteEvents(ctx, []*egopb.Event{event}))

		eventStream := eventstream.New()

		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Decrypt(mock.Anything, persistenceID, []byte("fake-cipher"), "key-1").Return(nil, assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with event unmarshal failure after decryption during recovery", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		encryptedEvent := &anypb.Any{TypeUrl: eventAny.GetTypeUrl(), Value: []byte("fake-cipher")}
		event := &egopb.Event{
			PersistenceId:   persistenceID,
			SequenceNumber:  1,
			Event:           encryptedEvent,
			Timestamp:       time.Now().Unix(),
			IsEncrypted:     true,
			EncryptionKeyId: "key-1",
		}
		require.NoError(t, eventStore.WriteEvents(ctx, []*egopb.Event{event}))

		eventStream := eventstream.New()

		// return garbage bytes so proto.Unmarshal of the Any fails
		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Decrypt(mock.Anything, persistenceID, []byte("fake-cipher"), "key-1").Return([]byte("not-valid-proto"), nil)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with event adapter chain failure during recovery", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		event := &egopb.Event{
			PersistenceId:  persistenceID,
			SequenceNumber: 1,
			Event:          eventAny,
			Timestamp:      time.Now().Unix(),
		}
		require.NoError(t, eventStore.WriteEvents(ctx, []*egopb.Event{event}))

		eventStream := eventstream.New()

		adapter := new(mockadapter.EventAdapter)
		adapter.EXPECT().Adapt(mock.Anything, uint64(1)).Return(nil, assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewEventAdapters([]eventadapter.EventAdapter{adapter}),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with event UnmarshalNew failure during recovery", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		// write an event with an unknown TypeUrl so UnmarshalNew fails
		event := &egopb.Event{
			PersistenceId:  persistenceID,
			SequenceNumber: 1,
			Event:          &anypb.Any{TypeUrl: "type.googleapis.com/unknown.TypeThatDoesNotExist", Value: []byte{}},
			Timestamp:      time.Now().Unix(),
		}
		require.NoError(t, eventStore.WriteEvents(ctx, []*egopb.Event{event}))

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with HandleEvent failure during recovery", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		eventStream := eventstream.New()

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		// pre-write an event that the behavior will fail to handle
		eventAny, err := anypb.New(&testpb.AccountCreated{AccountId: persistenceID, AccountBalance: 100})
		require.NoError(t, err)
		event := &egopb.Event{
			PersistenceId:  persistenceID,
			SequenceNumber: 1,
			Event:          eventAny,
			Timestamp:      time.Now().Unix(),
		}
		require.NoError(t, eventStore.WriteEvents(ctx, []*egopb.Event{event}))

		// use a behavior that returns an error from HandleEvent
		behavior := NewFailingHandleEventBehavior(persistenceID)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.Error(t, err)
		require.Nil(t, pid)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with event encryption failure during command processing", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Encrypt(mock.Anything, persistenceID, mock.Anything).Return(nil, "", assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with snapshot encryption failure during command processing", func(t *testing.T) {
		// Snapshot encryption failures are logged by the snapshot writer child
		// actor but do not fail the command. Snapshots are an optimization for
		// faster recovery, not a correctness requirement. The command succeeds
		// with a state reply.
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		encryptor := new(mockencryption.Encryptor)
		// event encryption (parent) succeeds; snapshot encryption (child) fails
		encryptor.EXPECT().Encrypt(mock.Anything, persistenceID, mock.Anything).Return([]byte("ciphertext"), "key-1", nil).Once()
		encryptor.EXPECT().Encrypt(mock.Anything, persistenceID, mock.Anything).Return(nil, "", assert.AnError).Maybe()

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		entityCfg := &extensions.EntityConfig{SnapshotInterval: 1}
		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior, entityCfg), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		// allow time for the child snapshot writer to process
		pause.For(time.Second)

		// verify no snapshot was written since encryption failed
		snap, err := snapshotStore.GetLatestSnapshot(ctx, persistenceID)
		require.NoError(t, err)
		assert.Nil(t, snap)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with DeleteEvents error in retention policy does not crash", func(t *testing.T) {
		ctx := context.TODO()

		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)
		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, persistenceID).Return(nil, nil)
		eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(nil)
		eventStore.EXPECT().DeleteEvents(mock.Anything, persistenceID, uint64(2)).Return(assert.AnError)

		snapshotStore := new(mocks.SnapshotStore)
		snapshotStore.EXPECT().Ping(mock.Anything).Return(nil)
		snapshotStore.EXPECT().GetLatestSnapshot(mock.Anything, persistenceID).Return(nil, nil)
		snapshotStore.EXPECT().WriteSnapshot(mock.Anything, mock.Anything).Return(nil)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
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

		entityCfg := &extensions.EntityConfig{
			SnapshotInterval:       2,
			HasRetentionPolicy:     true,
			DeleteEventsOnSnapshot: true,
			EventsRetentionCount:   0,
		}
		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior, entityCfg), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		// first command
		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		// second command triggers snapshot interval (2) and then DeleteEvents which errors
		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 100}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		// actor must still be alive: error is only logged
		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with DeleteSnapshots error in retention policy does not crash", func(t *testing.T) {
		ctx := context.TODO()

		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)
		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, persistenceID).Return(nil, nil)
		eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(nil).Times(4)

		snapshotStore := new(mocks.SnapshotStore)
		snapshotStore.EXPECT().Ping(mock.Anything).Return(nil)
		snapshotStore.EXPECT().GetLatestSnapshot(mock.Anything, persistenceID).Return(nil, nil)
		snapshotStore.EXPECT().WriteSnapshot(mock.Anything, mock.Anything).Return(nil).Times(2)
		snapshotStore.EXPECT().DeleteSnapshots(mock.Anything, persistenceID, uint64(2)).Return(assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
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

		entityCfg := &extensions.EntityConfig{
			SnapshotInterval:          2,
			HasRetentionPolicy:        true,
			DeleteSnapshotsOnSnapshot: true,
		}
		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor, goakt.WithDependencies(behavior, entityCfg), goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		// commands 1 and 2: first snapshot at seq 2
		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 100}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		// commands 3 and 4: second snapshot at seq 4 → DeleteSnapshots is called and errors
		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 50}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 25}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		// actor still alive after logged error
		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 4, state.StateReply.GetSequenceNumber())

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with unhandled non-command message does not crash", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior),
			goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		err = goakt.Tell(ctx, pid, new(egopb.NoReply))
		require.NoError(t, err)

		pause.For(500 * time.Millisecond)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("with persistEvents write failure shuts down actor", func(t *testing.T) {
		ctx := context.TODO()

		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)
		eventStream := eventstream.New()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, persistenceID).Return(nil, nil)
		eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior),
			goakt.WithLongLived())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
}

// noopEventAdapter is a no-op event adapter that passes events through unchanged
type noopEventAdapter struct{}

func (a *noopEventAdapter) Adapt(event *anypb.Any, _ uint64) (*anypb.Any, error) {
	return event, nil
}

// FailingHandleEventBehavior is a test behavior whose HandleEvent always returns an error.
type FailingHandleEventBehavior struct {
	id string
}

// NewFailingHandleEventBehavior creates a FailingHandleEventBehavior.
func NewFailingHandleEventBehavior(id string) *FailingHandleEventBehavior {
	return &FailingHandleEventBehavior{id: id}
}

func (f *FailingHandleEventBehavior) ID() string { return f.id }

func (f *FailingHandleEventBehavior) InitialState() State {
	return new(testpb.Account)
}

func (f *FailingHandleEventBehavior) HandleCommand(_ context.Context, command Command, _ State) ([]Event, error) {
	switch command.(type) {
	case *testpb.CreateAccount:
		return []Event{&testpb.AccountCreated{AccountId: f.id, AccountBalance: 100}}, nil
	}
	return nil, nil
}

func (f *FailingHandleEventBehavior) HandleEvent(_ context.Context, _ Event, _ State) (State, error) {
	return nil, assert.AnError
}

func (f *FailingHandleEventBehavior) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&egopb.StateReply{PersistenceId: f.id})
}

func (f *FailingHandleEventBehavior) UnmarshalBinary(data []byte) error {
	msg := new(egopb.StateReply)
	if err := proto.Unmarshal(data, msg); err != nil {
		return err
	}
	f.id = msg.GetPersistenceId()
	return nil
}

func TestEventSourcedActorBatch(t *testing.T) {
	t.Run("sequential commands flush by timer", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		err = actorSystem.Start(ctx)
		require.NoError(t, err)
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		resultingState := new(testpb.Account)
		require.NoError(t, state.StateReply.GetState().UnmarshalTo(resultingState))
		assert.EqualValues(t, 500, resultingState.GetAccountBalance())

		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 250}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state = commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, state.StateReply.GetSequenceNumber())

		resultingState = new(testpb.Account)
		require.NoError(t, state.StateReply.GetState().UnmarshalTo(resultingState))
		assert.EqualValues(t, 750, resultingState.GetAccountBalance())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("concurrent commands flush by threshold", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   2,
			BatchFlushWindow: 10 * time.Second,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		err = actorSystem.Start(ctx)
		require.NoError(t, err)
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		var wg sync.WaitGroup
		replies := make([]any, 2)
		errs := make([]error, 2)

		wg.Add(2)
		go func() {
			defer wg.Done()
			replies[0], errs[0] = goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 10*time.Second)
		}()
		go func() {
			defer wg.Done()
			replies[1], errs[1] = goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 300}, 10*time.Second)
		}()

		wg.Wait()

		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		for i, r := range replies {
			cr := r.(*egopb.CommandReply)
			require.IsType(t, new(egopb.CommandReply_StateReply), cr.GetReply(), "reply %d should be state reply", i)
		}

		stateReply, err := goakt.Ask(ctx, pid, &egopb.GetStateCommand{}, 5*time.Second)
		require.NoError(t, err)

		cr := stateReply.(*egopb.CommandReply)
		sr := cr.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, sr.StateReply.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("no-event command replies immediately in batch mode", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		err = actorSystem.Start(ctx)
		require.NoError(t, err)
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.TestNoEvent{}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 0, state.StateReply.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("error command replies immediately in batch mode", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		err = actorSystem.Start(ctx)
		require.NoError(t, err)
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: "wrong-id", Balance: 100}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		errorReply := commandReply.GetReply().(*egopb.CommandReply_ErrorReply)
		assert.Equal(t, "command sent to the wrong entity", errorReply.ErrorReply.GetMessage())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch with snapshot boundary crossing", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		entityCfg := &extensions.EntityConfig{
			SnapshotInterval: 2,
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		err = actorSystem.Start(ctx)
		require.NoError(t, err)
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		reply, err = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 100}, 5*time.Second)
		require.NoError(t, err)
		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, state.StateReply.GetSequenceNumber())

		pause.For(time.Second)

		snap, err := snapshotStore.GetLatestSnapshot(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, snap)
		assert.EqualValues(t, 2, snap.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("commands arriving during flush are stashed and processed", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   1,
			BatchFlushWindow: time.Second,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		err = actorSystem.Start(ctx)
		require.NoError(t, err)
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		var wg sync.WaitGroup
		const numCommands = 5
		replies := make([]any, numCommands)
		errs := make([]error, numCommands)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 100}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		wg.Add(numCommands)
		for i := range numCommands {
			go func(idx int) {
				defer wg.Done()
				replies[idx], errs[idx] = goakt.Ask(ctx, pid,
					&testpb.CreditAccount{AccountId: persistenceID, Balance: 10},
					10*time.Second)
			}(i)
		}

		wg.Wait()

		for i := range numCommands {
			require.NoError(t, errs[i], "command %d failed", i)
			cr := replies[i].(*egopb.CommandReply)
			require.IsType(t, new(egopb.CommandReply_StateReply), cr.GetReply(), "command %d should be state reply", i)
		}

		stateReply, err := goakt.Ask(ctx, pid, &egopb.GetStateCommand{}, 5*time.Second)
		require.NoError(t, err)

		cr := stateReply.(*egopb.CommandReply)
		sr := cr.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, numCommands+1, sr.StateReply.GetSequenceNumber())

		resultingState := new(testpb.Account)
		require.NoError(t, sr.StateReply.GetState().UnmarshalTo(resultingState))
		assert.EqualValues(t, 100+float64(numCommands)*10, resultingState.GetAccountBalance())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch persist failure returns error replies and shuts down actor", func(t *testing.T) {
		ctx := context.TODO()

		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, persistenceID).Return(nil, nil)
		eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(assert.AnError)

		eventStream := eventstream.New()

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   1,
			BatchFlushWindow: time.Second,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch persist failure with telemetry records metrics and ends spans", func(t *testing.T) {
		ctx := context.TODO()

		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil)
		eventStore.EXPECT().GetLatestEvent(mock.Anything, persistenceID).Return(nil, nil)
		eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(assert.AnError)

		eventStream := eventstream.New()

		noopTracer := tracenoop.NewTracerProvider().Tracer("test")
		noopMeter := noop.NewMeterProvider().Meter("test")

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   1,
			BatchFlushWindow: time.Second,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewTelemetryExtension(noopTracer, noopMeter),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch mode with encryption failure in processAndBatch", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Encrypt(mock.Anything, persistenceID, mock.Anything).Return(nil, "", assert.AnError)

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch mode with telemetry traces commands", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		noopTracer := tracenoop.NewTracerProvider().Tracer("test")
		noopMeter := noop.NewMeterProvider().Meter("test")

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewTelemetryExtension(noopTracer, noopMeter),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch mode with telemetry handles no-event command", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		noopTracer := tracenoop.NewTracerProvider().Tracer("test")
		noopMeter := noop.NewMeterProvider().Meter("test")

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewTelemetryExtension(noopTracer, noopMeter),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.TestNoEvent{}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch mode with telemetry handles error command", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		noopTracer := tracenoop.NewTracerProvider().Tracer("test")
		noopMeter := noop.NewMeterProvider().Meter("test")

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewTelemetryExtension(noopTracer, noopMeter),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: "wrong-id", Balance: 100}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch mode with default flush window when only threshold is set", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		entityCfg := &extensions.EntityConfig{
			BatchThreshold: 100,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("unhandled non-command message in batch mode", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		err = goakt.Tell(ctx, pid, new(egopb.NoReply))
		require.NoError(t, err)

		pause.For(500 * time.Millisecond)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch mode with encryption failure and telemetry ends span", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		noopTracer := tracenoop.NewTracerProvider().Tracer("test")
		noopMeter := noop.NewMeterProvider().Meter("test")

		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Encrypt(mock.Anything, persistenceID, mock.Anything).Return(nil, "", assert.AnError)

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   100,
			BatchFlushWindow: 100 * time.Millisecond,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewEncryptor(encryptor),
				extensions.NewTelemetryExtension(noopTracer, noopMeter),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("multiple commands batch with telemetry covers reply span and timer dedup", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		noopTracer := tracenoop.NewTracerProvider().Tracer("test")
		noopMeter := noop.NewMeterProvider().Meter("test")

		entityCfg := &extensions.EntityConfig{
			BatchThreshold:   3,
			BatchFlushWindow: 10 * time.Second,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewTelemetryExtension(noopTracer, noopMeter),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		var wg sync.WaitGroup
		replies := make([]any, 3)
		errs := make([]error, 3)

		wg.Add(3)
		go func() {
			defer wg.Done()
			replies[0], errs[0] = goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 10*time.Second)
		}()
		go func() {
			defer wg.Done()
			replies[1], errs[1] = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 100}, 10*time.Second)
		}()
		go func() {
			defer wg.Done()
			replies[2], errs[2] = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 50}, 10*time.Second)
		}()

		wg.Wait()

		for i, e := range errs {
			require.NoError(t, e, "command %d failed", i)
			cr := replies[i].(*egopb.CommandReply)
			require.IsType(t, new(egopb.CommandReply_StateReply), cr.GetReply(), "command %d should be state reply", i)
		}

		stateReply, err := goakt.Ask(ctx, pid, &egopb.GetStateCommand{}, 5*time.Second)
		require.NoError(t, err)

		cr := stateReply.(*egopb.CommandReply)
		sr := cr.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 3, sr.StateReply.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
	t.Run("batch with snapshot and telemetry", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		snapshotStore := testkit.NewSnapshotStore()
		persistenceID := uuid.NewString()
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		require.NoError(t, eventStore.Connect(ctx))
		require.NoError(t, snapshotStore.Connect(ctx))
		pause.For(time.Second)

		eventStream := eventstream.New()

		noopTracer := tracenoop.NewTracerProvider().Tracer("test")
		noopMeter := noop.NewMeterProvider().Meter("test")

		entityCfg := &extensions.EntityConfig{
			SnapshotInterval: 2,
			BatchThreshold:   2,
			BatchFlushWindow: 10 * time.Second,
		}

		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
				extensions.NewTelemetryExtension(noopTracer, noopMeter),
			),
			goakt.WithActorInitMaxRetries(3))
		require.NoError(t, err)

		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		actor := newEventSourcedActor()
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), actor,
			goakt.WithDependencies(behavior, entityCfg),
			goakt.WithLongLived(),
			goakt.WithStashing())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		var wg sync.WaitGroup
		replies := make([]any, 2)
		errs := make([]error, 2)

		wg.Add(2)
		go func() {
			defer wg.Done()
			replies[0], errs[0] = goakt.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500}, 10*time.Second)
		}()
		go func() {
			defer wg.Done()
			replies[1], errs[1] = goakt.Ask(ctx, pid, &testpb.CreditAccount{AccountId: persistenceID, Balance: 100}, 10*time.Second)
		}()

		wg.Wait()

		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		for i, r := range replies {
			cr := r.(*egopb.CommandReply)
			require.IsType(t, new(egopb.CommandReply_StateReply), cr.GetReply(), "reply %d should be state reply", i)
		}

		pause.For(time.Second)

		snap, err := snapshotStore.GetLatestSnapshot(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, snap)
		assert.EqualValues(t, 2, snap.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		pause.For(time.Second)
		require.NoError(t, actorSystem.Stop(ctx))
	})
}
