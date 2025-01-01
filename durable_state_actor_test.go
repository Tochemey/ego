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
	"github.com/tochemey/goakt/v2/actors"
	"github.com/tochemey/goakt/v2/log"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/internal/lib"
	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
	"github.com/tochemey/ego/v3/testkit"
)

func TestDurableStateBehavior(t *testing.T) {
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

		durableStore := testkit.NewDurableStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)
		err = durableStore.Connect(ctx)
		require.NoError(t, err)

		// create an instance of events stream
		eventStream := eventstream.New()

		actor := newDurableStateActor(behavior, durableStore, eventStream)
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
		require.True(t, proto.Equal(expected, resultingState))

		// stop the actor system
		err = actorSystem.Stop(ctx)
		require.NoError(t, err)

		err = durableStore.Disconnect(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)
		eventStream.Close()
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

		durableStore := testkit.NewDurableStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountDurableStateBehavior(persistenceID)

		err = durableStore.Connect(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// create an instance of events stream
		eventStream := eventstream.New()

		// create the persistence actor using the behavior previously created
		persistentActor := newDurableStateActor(behavior, durableStore, eventStream)
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

		err = actorSystem.Stop(ctx)
		require.NoError(t, err)

		err = durableStore.Disconnect(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)
		eventStream.Close()
	})
	t.Run("with state recovery from state store", func(t *testing.T) {
		ctx := context.TODO()
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

		durableStore := testkit.NewDurableStore()
		require.NoError(t, durableStore.Connect(ctx))

		lib.Pause(time.Second)

		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)

		err = durableStore.Connect(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		eventStream := eventstream.New()

		persistentActor := newDurableStateActor(behavior, durableStore, eventStream)
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor)
		require.NoError(t, err)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}

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

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)

		lib.Pause(time.Second)

		// free resources
		assert.NoError(t, durableStore.Disconnect(ctx))
		eventStream.Close()
	})
}
