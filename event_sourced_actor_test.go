/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
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
	"github.com/tochemey/goakt/v2/actors"
	"github.com/tochemey/goakt/v2/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/internal/lib"
	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
	"github.com/tochemey/ego/v3/testkit"
)

func TestEventSourcedActor(t *testing.T) {
	t.Run("with state reply", func(t *testing.T) {
		ctx := context.TODO()
		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err = eventStore.Connect(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor(behavior, eventStore, eventStream)
		// spawn the actor
		pid, _ := actorSystem.Spawn(ctx, behavior.ID(), actor)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}
		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, command, 5*time.Second)
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
		reply, err = actors.Ask(ctx, pid, command, 5*time.Second)
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

		lib.Pause(time.Second)

		// close the stream
		eventStream.Close()
		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with error reply", func(t *testing.T) {
		ctx := context.TODO()

		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err = eventStore.Connect(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create the persistence actor using the behavior previously created
		persistentActor := newEventSourcedActor(behavior, eventStore, eventStream)
		// spawn the actor
		pid, _ := actorSystem.Spawn(ctx, behavior.ID(), persistentActor)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}
		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, command, time.Second)
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
		reply, err = actors.Ask(ctx, pid, command, time.Second)
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

		lib.Pause(time.Second)

		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with unhandled command", func(t *testing.T) {
		ctx := context.TODO()

		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err = eventStore.Connect(ctx)
		require.NoError(t, err)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create the persistence actor using the behavior previously created
		persistentActor := newEventSourcedActor(behavior, eventStore, eventStream)
		// spawn the actor
		pid, _ := actorSystem.Spawn(ctx, behavior.ID(), persistentActor)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

		command := &testpb.TestSend{}
		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, command, time.Second)
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

		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3),
		)
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		lib.Pause(time.Second)

		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err = eventStore.Connect(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create an instance of event stream
		eventStream := eventstream.New()

		// create the persistence actor using the behavior previously created
		persistentActor := newEventSourcedActor(behavior, eventStore, eventStream)
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor)
		require.NoError(t, err)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}
		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, command, time.Second)
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
		reply, err = actors.Ask(ctx, pid, command, time.Second)
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
		lib.Pause(time.Second)

		// restart the actor
		pid, err = actorSystem.ReSpawn(ctx, behavior.ID())
		require.NoError(t, err)

		lib.Pause(time.Second)

		// fetch the current state
		command = &egopb.GetStateCommand{}
		reply, err = actors.Ask(ctx, pid, command, time.Second)
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

		lib.Pause(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with no event to persist", func(t *testing.T) {
		ctx := context.TODO()

		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err = eventStore.Connect(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create an instance of event stream
		eventStream := eventstream.New()

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor(behavior, eventStore, eventStream)
		// spawn the actor
		pid, _ := actorSystem.Spawn(ctx, behavior.ID(), actor)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}
		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, command, 5*time.Second)
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
		reply, err = actors.Ask(ctx, pid, command, 5*time.Second)
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
		reply, err = actors.Ask(ctx, pid, command, 5*time.Second)
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

		lib.Pause(time.Second)

		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with unhandled event", func(t *testing.T) {
		ctx := context.TODO()
		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create the event store
		eventStore := testkit.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEventSourcedBehavior(persistenceID)

		// connect the event store
		err = eventStore.Connect(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create the persistence actor using the behavior previously created
		actor := newEventSourcedActor(behavior, eventStore, eventStream)
		// spawn the actor
		pid, _ := actorSystem.Spawn(ctx, behavior.ID(), actor)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, &testpb.CreateAccount{AccountBalance: 500.00}, 5*time.Second)
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

		reply, err = actors.Ask(ctx, pid, new(emptypb.Empty), 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		errorReply := commandReply.GetReply().(*egopb.CommandReply_ErrorReply)
		assert.Equal(t, "unhandled event", errorReply.ErrorReply.GetMessage())

		// disconnect from the event store
		require.NoError(t, eventStore.Disconnect(ctx))

		lib.Pause(time.Second)

		// close the stream
		eventStream.Close()
		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
}
