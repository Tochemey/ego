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
	"fmt"
	"math"
	"time"

	"github.com/tochemey/ego/v3/internal/ticker"
	"github.com/tochemey/goakt/v2/actors"
	"github.com/tochemey/goakt/v2/goaktpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/internal/errorschain"
	"github.com/tochemey/ego/v3/persistence"
)

var (
	statesTopic = "topic.states.%d"
)

// durableStateActor is a durable state based actor
type durableStateActor struct {
	DurableStateBehavior
	stateStore      persistence.StateStore
	currentState    State
	lastSyncVersion uint64
	currentVersion  uint64
	lastCommandTime time.Time
	eventsStream    eventstream.Stream
	actorSystem     actors.ActorSystem
	bufferedWrite   *int32
}

// implements the actors.Actor interface
var _ actors.Actor = (*durableStateActor)(nil)

// newDurableStateActor creates an instance of actor provided the DurableStateBehavior
func newDurableStateActor(behavior DurableStateBehavior, stateStore persistence.StateStore, eventsStream eventstream.Stream) *durableStateActor {
	return &durableStateActor{
		stateStore:           stateStore,
		eventsStream:         eventsStream,
		DurableStateBehavior: behavior,
	}
}

func (entity *durableStateActor) WithBufferedWrites(interval *int32) *durableStateActor {
	if interval != nil {
		entity.bufferedWrite = interval
	}
	return entity
}

// PreStart pre-starts the actor
func (entity *durableStateActor) PreStart(ctx context.Context) error {
	if entity.bufferedWrite != nil {
		go entity.periodicallyPersistData(ctx)
	}

	return errorschain.
		New(errorschain.ReturnFirst()).
		AddError(entity.durableStateRequired()).
		AddError(entity.stateStore.Ping(ctx)).
		AddError(entity.recoverFromStore(ctx)).
		Error()
}

func (entity *durableStateActor) periodicallyPersistData(ctx context.Context) {
	// Run a go channel that emits message every X interval to persist the data in the store

	ticker := ticker.New(5 * time.Second)
	ticker.Start()
	// tickerStopSignal := make(chan struct{}, 1)

	go func() {
		for range ticker.Ticks {
			entity.persistState(ctx)
		}
	}()

}

// Receive processes any message dropped into the actor mailbox.
func (entity *durableStateActor) Receive(ctx *actors.ReceiveContext) {
	switch command := ctx.Message().(type) {
	case *goaktpb.PostStart:
		entity.actorSystem = ctx.ActorSystem()
	case *egopb.GetStateCommand:
		entity.sendStateReply(ctx)
	default:
		entity.processCommand(ctx, command)
	}
}

// PostStop prepares the actor to gracefully shutdown
func (entity *durableStateActor) PostStop(ctx context.Context) error {
	return errorschain.
		New(errorschain.ReturnFirst()).
		AddError(entity.stateStore.Ping(ctx)).
		AddError(entity.persistState(ctx)).
		Error()
}

// recoverFromStore reset the persistent actor to the latest state in case there is one
// this is vital when the entity actor is restarting.
func (entity *durableStateActor) recoverFromStore(ctx context.Context) error {
	durableState, err := entity.stateStore.GetLatestState(ctx, entity.ID())
	if err != nil {
		return fmt.Errorf("failed unmarshal the latest state: %w", err)
	}

	if durableState != nil && proto.Equal(durableState, new(egopb.DurableState)) {
		currentState := entity.InitialState()
		if err := durableState.GetResultingState().UnmarshalTo(currentState); err != nil {
			return fmt.Errorf("failed unmarshal the latest state: %w", err)
		}

		entity.currentState = currentState
		entity.currentVersion = durableState.GetVersionNumber()
		return nil
	}

	entity.currentState = entity.InitialState()
	return nil
}

// processCommand processes the incoming command
func (entity *durableStateActor) processCommand(receiveContext *actors.ReceiveContext, command Command) {
	ctx := receiveContext.Context()
	newState, newVersion, err := entity.HandleCommand(ctx, command, entity.currentVersion, entity.currentState)
	if err != nil {
		entity.sendErrorReply(receiveContext, err)
		return
	}

	// check whether the pre-conditions have met
	if err := entity.checkPreconditions(newState, newVersion); err != nil {
		entity.sendErrorReply(receiveContext, err)
		return
	}

	// set the current state with the newState
	entity.currentState = newState
	entity.lastCommandTime = timestamppb.Now().AsTime()
	entity.currentVersion = newVersion

	if entity.bufferedWrite == nil {
		if err := entity.persistState(ctx); err != nil {
			entity.sendErrorReply(receiveContext, err)
			return
		}
	}

	if err := entity.publishToStream(); err != nil {
		entity.sendErrorReply(receiveContext, err)
		return
	}

	entity.sendStateReply(receiveContext)
}

// sendStateReply sends a state reply message
func (entity *durableStateActor) sendStateReply(ctx *actors.ReceiveContext) {
	state, _ := anypb.New(entity.currentState)
	ctx.Response(&egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.ID(),
				State:          state,
				SequenceNumber: entity.currentVersion,
				Timestamp:      entity.lastCommandTime.Unix(),
			},
		},
	})
}

// sendErrorReply sends an error as a reply message
func (entity *durableStateActor) sendErrorReply(ctx *actors.ReceiveContext, err error) {
	ctx.Response(&egopb.CommandReply{
		Reply: &egopb.CommandReply_ErrorReply{
			ErrorReply: &egopb.ErrorReply{
				Message: err.Error(),
			},
		},
	})
}

// checkAndSetPreconditions validates the newState and the newVersion
func (entity *durableStateActor) checkPreconditions(newState State, newVersion uint64) error {
	currentState := entity.currentState
	currentStateType := currentState.ProtoReflect().Descriptor().FullName()
	latestStateType := newState.ProtoReflect().Descriptor().FullName()
	if currentStateType != latestStateType {
		return fmt.Errorf("mismatch state types: %s != %s", currentStateType, latestStateType)
	}

	proceed := int(math.Abs(float64(newVersion-entity.currentVersion))) == 1
	if !proceed {
		return fmt.Errorf("%s received version=(%d) while current version is (%d)",
			entity.ID(),
			newVersion,
			entity.currentVersion)
	}
	return nil
}

// checks whether the durable state store is set or not
func (entity *durableStateActor) durableStateRequired() error {
	if entity.stateStore == nil {
		return ErrDurableStateStoreRequired
	}
	return nil
}

// persistState persists the actor state
func (entity *durableStateActor) persistState(ctx context.Context) error {
	resultingState, _ := anypb.New(entity.currentState)
	shardNumber := entity.actorSystem.GetPartition(entity.ID())

	if entity.currentVersion != entity.lastSyncVersion {
		fmt.Println("Persisting state")
		durableState := &egopb.DurableState{
			PersistenceId:  entity.ID(),
			VersionNumber:  entity.currentVersion,
			ResultingState: resultingState,
			Timestamp:      entity.lastCommandTime.Unix(),
			Shard:          uint64(shardNumber),
		}
		entity.lastSyncVersion = entity.currentVersion
		return entity.stateStore.WriteState(ctx, durableState)
	}
	return nil
}

// publish event to actor stream
func (entity *durableStateActor) publishToStream() error {
	resultingState, _ := anypb.New(entity.currentState)
	shardNumber := entity.actorSystem.GetPartition(entity.ID())
	topic := fmt.Sprintf(statesTopic, shardNumber)

	durableState := &egopb.DurableState{
		PersistenceId:  entity.ID(),
		VersionNumber:  entity.currentVersion,
		ResultingState: resultingState,
		Timestamp:      entity.lastCommandTime.Unix(),
		Shard:          uint64(shardNumber),
	}

	entity.eventsStream.Publish(topic, durableState)
	return nil
}
