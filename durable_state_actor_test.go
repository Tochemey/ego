/*
 * MIT License
 *
 * Copyright (c) 2023-2025 Tochemey
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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/log"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/internal/lib"
	mocks "github.com/tochemey/ego/v3/mocks/persistence"
	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
	"github.com/tochemey/ego/v3/testkit"
)

func TestDurableStateBehavior(t *testing.T) {
	t.Run("with state reply", func(t *testing.T) {
		ctx := context.TODO()
		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithPassivationDisabled(),
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithActorInitMaxRetries(3))
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

		lib.Pause(time.Second)
		eventStream.Close()
	})
	t.Run("with error reply", func(t *testing.T) {
		ctx := context.TODO()

		// create an actor system
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithPassivationDisabled(),
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithActorInitMaxRetries(3))
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

		lib.Pause(time.Second)
		eventStream.Close()
	})
	t.Run("with state recovery from state store", func(t *testing.T) {
		ctx := context.TODO()
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithPassivationDisabled(),
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithActorInitMaxRetries(3),
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
		lib.Pause(time.Second)

		// restart the actor
		pid, err = actorSystem.ReSpawn(ctx, behavior.ID())
		require.NoError(t, err)

		lib.Pause(time.Second)

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

		lib.Pause(time.Second)

		// free resources
		assert.NoError(t, durableStore.Disconnect(ctx))
		eventStream.Close()
	})
	t.Run("with state recovery from state store with no latest state", func(t *testing.T) {
		ctx := context.TODO()
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithPassivationDisabled(),
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithActorInitMaxRetries(3),
		)
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)

		eventStream := eventstream.New()

		durableStore := new(mocks.StateStore)
		durableStore.EXPECT().Ping(mock.Anything).Return(nil)
		durableStore.EXPECT().GetLatestState(mock.Anything, behavior.ID()).Return(new(egopb.DurableState), nil)
		durableStore.EXPECT().WriteState(mock.Anything, mock.AnythingOfType("*egopb.DurableState")).Return(nil)

		persistentActor := newDurableStateActor(behavior, durableStore, eventStream)
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor)
		require.NoError(t, err)
		require.NotNil(t, pid)

		lib.Pause(time.Second)

		err = actorSystem.Stop(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		eventStream.Close()
		durableStore.AssertExpectations(t)
	})
	t.Run("with state recovery from state store failure", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithPassivationDisabled(),
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithActorInitMaxRetries(3),
		)
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)

		eventStream := eventstream.New()

		durableStore := new(mocks.StateStore)
		durableStore.EXPECT().Ping(mock.Anything).Return(nil)
		durableStore.EXPECT().GetLatestState(mock.Anything, behavior.ID()).Return(nil, assert.AnError)

		persistentActor := newDurableStateActor(behavior, durableStore, eventStream)
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor)
		require.Error(t, err)
		require.Nil(t, pid)

		lib.Pause(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)

		lib.Pause(time.Second)
		eventStream.Close()
		durableStore.AssertExpectations(t)
	})
	t.Run("with state recovery from state store with initial parsing failure", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithPassivationDisabled(),
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithActorInitMaxRetries(3),
		)
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

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

		persistentActor := newDurableStateActor(behavior, durableStore, eventStream)
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor)
		require.Error(t, err)
		require.Nil(t, pid)

		lib.Pause(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)

		lib.Pause(time.Second)

		eventStream.Close()
		durableStore.AssertExpectations(t)
	})

	t.Run("with no durable state store", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		actorSystem, err := goakt.NewActorSystem("TestActorSystem",
			goakt.WithPassivationDisabled(),
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithActorInitMaxRetries(3),
		)
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		persistenceID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(persistenceID)

		eventStream := eventstream.New()

		persistentActor := newDurableStateActor(behavior, nil, eventStream)
		pid, err := actorSystem.Spawn(ctx, behavior.ID(), persistentActor)
		require.Error(t, err)
		require.Nil(t, pid)

		lib.Pause(time.Second)

		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)

		lib.Pause(time.Second)

		eventStream.Close()
	})
}
