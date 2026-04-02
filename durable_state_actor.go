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
	"math"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/runner"
	"github.com/tochemey/ego/v4/persistence"
)

var (
	statesTopic = "topic.states.%d"
)

// DurableStateActor is a durable state based actor
type DurableStateActor struct {
	behavior        DurableStateBehavior
	stateStore      persistence.StateStore
	currentState    State
	cachedStateAny  *anypb.Any // cached marshal of currentState, invalidated on state change
	currentVersion  uint64
	lastCommandTime time.Time
	eventsStream    eventstream.Stream
	actorSystem     goakt.ActorSystem
	persistenceID   string
	tracer          trace.Tracer
	metrics         *metrics

	// Cached values computed once at startup to avoid per-command allocations.
	shardNumber uint64
	statesTopic string
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

	if ext := ctx.Extension(extensions.TelemetryExtensionID); ext != nil {
		telExt := ext.(*extensions.TelemetryExtension)
		entity.tracer = telExt.Tracer()
		entity.metrics = newMetrics(telExt.Meter())
	}

	if err := runner.
		New(runner.WithFailFast()).
		AddRunner(entity.durableStateRequired).
		AddRunner(func() error {
			if entity.behavior == nil {
				return fmt.Errorf("behavior is required")
			}
			return nil
		}).
		AddRunner(func() error { return entity.stateStore.Ping(ctx.Context()) }).
		AddRunner(func() error { return entity.recoverFromStore(ctx.Context()) }).
		Run(); err != nil {
		return err
	}

	if entity.metrics != nil {
		entity.metrics.entitiesActive.Add(ctx.Context(), 1)
	}

	return nil
}

// Receive processes any message dropped into the actor mailbox.
func (entity *DurableStateActor) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		entity.actorSystem = ctx.ActorSystem()
		entity.shardNumber = ctx.ActorSystem().Partition(entity.persistenceID)
		entity.statesTopic = fmt.Sprintf(statesTopic, entity.shardNumber)
	case *egopb.GetStateCommand:
		entity.sendStateReply(ctx)
	default:
		msg := message.(Command)
		entity.processCommand(ctx, msg)
	}
}

// PostStop prepares the actor to gracefully shutdown
func (entity *DurableStateActor) PostStop(ctx *goakt.Context) error {
	if entity.metrics != nil {
		entity.metrics.entitiesActive.Add(ctx.Context(), -1)
	}
	return runner.
		New(runner.WithFailFast()).
		AddRunner(func() error { return entity.stateStore.Ping(ctx.Context()) }).
		AddRunner(func() error { return entity.persistStateAndPublish(ctx.Context()) }).
		Run()
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
	startTime := time.Now()

	if entity.tracer != nil {
		var span trace.Span
		ctx, span = entity.tracer.Start(ctx, "ego.command",
			trace.WithAttributes(
				attribute.String("ego.persistence_id", entity.persistenceID),
				attribute.String("ego.command_type", string(command.ProtoReflect().Descriptor().FullName())),
			))
		defer span.End()
	}

	if entity.metrics != nil {
		entity.metrics.commandsTotal.Add(ctx, 1)
		defer func() {
			duration := float64(time.Since(startTime).Milliseconds())
			entity.metrics.commandsDuration.Record(ctx, duration)
		}()
	}

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
	entity.cachedStateAny, _ = anypb.New(newState) // eagerly cache for the reply and persist that follow
	entity.lastCommandTime = time.Now()
	entity.currentVersion = newVersion

	if err := entity.persistStateAndPublish(ctx); err != nil {
		entity.sendErrorReply(receiveContext, err)
		return
	}

	entity.sendStateReply(receiveContext)
}

// currentStateAny returns the cached anypb.Any of currentState, computing it
// only when the state has changed since the last call.
func (entity *DurableStateActor) currentStateAny() *anypb.Any {
	if entity.cachedStateAny == nil {
		entity.cachedStateAny, _ = anypb.New(entity.currentState)
	}
	return entity.cachedStateAny
}

// sendStateReply sends a state reply message
func (entity *DurableStateActor) sendStateReply(ctx *goakt.ReceiveContext) {
	ctx.Response(&egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.persistenceID,
				State:          entity.currentStateAny(),
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

// persistStateAndPublish persists the actor state and publishes it to the stream.
func (entity *DurableStateActor) persistStateAndPublish(ctx context.Context) error {
	durableState := &egopb.DurableState{
		PersistenceId:  entity.persistenceID,
		VersionNumber:  entity.currentVersion,
		ResultingState: entity.currentStateAny(),
		Timestamp:      entity.lastCommandTime.Unix(),
		Shard:          entity.shardNumber,
	}

	if err := entity.stateStore.WriteState(ctx, durableState); err != nil {
		return err
	}

	entity.eventsStream.Publish(entity.statesTopic, durableState)
	return nil
}
