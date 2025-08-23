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
	"fmt"
	"math"
	"time"

	goakt "github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/internal/extensions"
	"github.com/tochemey/ego/v3/internal/runner"
	"github.com/tochemey/ego/v3/persistence"
)

var (
	statesTopic = "topic.states.%d"
)

// DurableStateActor is a durable state based actor
type DurableStateActor struct {
	behavior        DurableStateBehavior
	stateStore      persistence.StateStore
	currentState    State
	currentVersion  uint64
	lastCommandTime time.Time
	eventsStream    eventstream.Stream
	actorSystem     goakt.ActorSystem
	persistenceID   string
}

// implements the goakt.Actor interface
var _ goakt.Actor = (*DurableStateActor)(nil)

// newDurableStateActor creates an instance of an actor provided the DurableStateBehavior
func newDurableStateActor() *DurableStateActor {
	return &DurableStateActor{}
}

// PreStart pre-starts the actor
func (entity *DurableStateActor) PreStart(ctx *goakt.Context) error {
	entity.stateStore = ctx.Extension(extensions.DurableStateStoreExtensionID).(*extensions.DurableStateStore).Underlying()
	entity.eventsStream = ctx.Extension(extensions.EventsStreamExtensionID).(*extensions.EventsStream).Underlying()
	entity.persistenceID = ctx.ActorName()

	for _, dependency := range ctx.Dependencies() {
		if dependency != nil {
			if behavior, ok := dependency.(DurableStateBehavior); ok {
				entity.behavior = behavior
				break
			}
		}
	}

	return runner.
		New(runner.ReturnFirst()).
		AddFunc(entity.durableStateRequired).
		AddFunc(func() error {
			if entity.behavior == nil {
				return fmt.Errorf("behavior is required")
			}
			return nil
		}).
		AddFunc(func() error { return entity.stateStore.Ping(ctx.Context()) }).
		AddFunc(func() error { return entity.recoverFromStore(ctx.Context()) }).
		Error()
}

// Receive processes any message dropped into the actor mailbox.
func (entity *DurableStateActor) Receive(ctx *goakt.ReceiveContext) {
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
func (entity *DurableStateActor) PostStop(ctx *goakt.Context) error {
	return runner.
		New(runner.ReturnFirst()).
		AddFunc(func() error { return entity.stateStore.Ping(ctx.Context()) }).
		AddFunc(func() error { return entity.persistStateAndPublish(ctx.Context()) }).
		Error()
}

// recoverFromStore reset the persistent actor to the latest state in case there is one
// this is vital when the entity actor is restarting.
func (entity *DurableStateActor) recoverFromStore(ctx context.Context) error {
	durableState, err := entity.stateStore.GetLatestState(ctx, entity.persistenceID)
	if err != nil {
		return fmt.Errorf("failed to get the latest state: %w", err)
	}

	if durableState == nil || proto.Equal(durableState, new(egopb.DurableState)) {
		entity.currentState = entity.behavior.InitialState()
		return nil
	}

	currentState := entity.behavior.InitialState()
	if resultingState := durableState.GetResultingState(); resultingState != nil {
		if err := resultingState.UnmarshalTo(currentState); err != nil {
			return fmt.Errorf("failed to unmarshal the latest state: %w", err)
		}
	}

	entity.currentState = currentState
	entity.currentVersion = durableState.GetVersionNumber()
	return nil
}

// processCommand processes the incoming command
func (entity *DurableStateActor) processCommand(receiveContext *goakt.ReceiveContext, command Command) {
	ctx := receiveContext.Context()
	newState, newVersion, err := entity.behavior.HandleCommand(ctx, command, entity.currentVersion, entity.currentState)
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

	if err := entity.persistStateAndPublish(ctx); err != nil {
		entity.sendErrorReply(receiveContext, err)
		return
	}

	entity.sendStateReply(receiveContext)
}

// sendStateReply sends a state reply message
func (entity *DurableStateActor) sendStateReply(ctx *goakt.ReceiveContext) {
	state, _ := anypb.New(entity.currentState)
	ctx.Response(&egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.persistenceID,
				State:          state,
				SequenceNumber: entity.currentVersion,
				Timestamp:      entity.lastCommandTime.Unix(),
			},
		},
	})
}

// sendErrorReply sends an error as a reply message
func (entity *DurableStateActor) sendErrorReply(ctx *goakt.ReceiveContext, err error) {
	ctx.Response(&egopb.CommandReply{
		Reply: &egopb.CommandReply_ErrorReply{
			ErrorReply: &egopb.ErrorReply{
				Message: err.Error(),
			},
		},
	})
}

// checkAndSetPreconditions validates the newState and the newVersion
func (entity *DurableStateActor) checkPreconditions(newState State, newVersion uint64) error {
	currentState := entity.currentState
	currentStateType := currentState.ProtoReflect().Descriptor().FullName()
	latestStateType := newState.ProtoReflect().Descriptor().FullName()
	if currentStateType != latestStateType {
		return fmt.Errorf("mismatch state types: %s != %s", currentStateType, latestStateType)
	}

	proceed := int(math.Abs(float64(newVersion-entity.currentVersion))) == 1
	if !proceed {
		return fmt.Errorf("%s received version=(%d) while current version is (%d)",
			entity.persistenceID,
			newVersion,
			entity.currentVersion)
	}
	return nil
}

// checks whether the durable state store is set or not
func (entity *DurableStateActor) durableStateRequired() error {
	if entity.stateStore == nil {
		return ErrDurableStateStoreRequired
	}
	return nil
}

// persistState persists the actor state
func (entity *DurableStateActor) persistStateAndPublish(ctx context.Context) error {
	resultingState, _ := anypb.New(entity.currentState)
	shardNumber := entity.actorSystem.GetPartition(entity.persistenceID)
	topic := fmt.Sprintf(statesTopic, shardNumber)

	entity.actorSystem.Logger().Debugf("publishing durableState to topic: %s", topic)

	durableState := &egopb.DurableState{
		PersistenceId:  entity.persistenceID,
		VersionNumber:  entity.currentVersion,
		ResultingState: resultingState,
		Timestamp:      entity.lastCommandTime.Unix(),
		Shard:          uint64(shardNumber),
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		entity.eventsStream.Publish(topic, durableState)
		return nil
	})

	eg.Go(func() error {
		return entity.stateStore.WriteState(ctx, durableState)
	})

	return eg.Wait()
}
