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
	"time"

	goakt "github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/internal/extensions"
	"github.com/tochemey/ego/v3/internal/runner"
	"github.com/tochemey/ego/v3/persistence"
)

var (
	eventsTopic = "topic.events.%d"
)

// EventSourcedActor is an event sourced based actor
type EventSourcedActor struct {
	behavior        EventSourcedBehavior
	eventsStore     persistence.EventsStore
	currentState    State
	eventsCounter   uint64
	lastCommandTime time.Time
	eventsStream    eventstream.Stream
	persistenceID   string
}

// implements the goakt.Actor interface
var _ goakt.Actor = (*EventSourcedActor)(nil)

// newEventSourcedActor creates an instance of actor provided the eventSourcedHandler and the events store
func newEventSourcedActor() *EventSourcedActor {
	return &EventSourcedActor{}
}

// PreStart pre-starts the actor
func (entity *EventSourcedActor) PreStart(ctx *goakt.Context) error {
	entity.eventsStore = ctx.Extension(extensions.EventsStoreExtensionID).(*extensions.EventsStore).Underlying()
	entity.eventsStream = ctx.Extension(extensions.EventsStreamExtensionID).(*extensions.EventsStream).Underlying()
	entity.persistenceID = ctx.ActorName()

	for _, dependency := range ctx.Dependencies() {
		if dependency != nil {
			if behavior, ok := dependency.(EventSourcedBehavior); ok {
				entity.behavior = behavior
				break
			}
		}
	}

	return runner.
		New(runner.WithFailFast()).
		AddRunner(func() error {
			if entity.behavior == nil {
				return fmt.Errorf("behavior is required")
			}
			return nil
		}).
		AddRunner(func() error { return entity.eventsStore.Ping(ctx.Context()) }).
		AddRunner(func() error { return entity.recoverFromSnapshot(ctx.Context()) }).
		Run()
}

// Receive processes any message dropped into the actor mailbox.
func (entity *EventSourcedActor) Receive(ctx *goakt.ReceiveContext) {
	switch command := ctx.Message().(type) {
	case *goaktpb.PostStart:
		// pass
	case *egopb.GetStateCommand:
		entity.getStateAndReply(ctx)
	default:
		entity.processCommandAndReply(ctx, command)
	}
}

// PostStop prepares the actor to gracefully shutdown
// nolint
func (entity *EventSourcedActor) PostStop(*goakt.Context) error {
	entity.eventsCounter = 0
	return nil
}

// recoverFromSnapshot reset the persistent actor to the latest snapshot in case there is one
// this is vital when the entity actor is restarting.
func (entity *EventSourcedActor) recoverFromSnapshot(ctx context.Context) error {
	event, err := entity.eventsStore.GetLatestEvent(ctx, entity.persistenceID)
	if err != nil {
		return fmt.Errorf("failed to recover the latest journal: %w", err)
	}

	if event == nil || event.GetResultingState() == nil {
		entity.currentState = entity.behavior.InitialState()
		return nil
	}

	currentState := entity.behavior.InitialState()
	if err := event.GetResultingState().UnmarshalTo(currentState); err != nil {
		return fmt.Errorf("failed to unmarshal the latest state: %w", err)
	}

	entity.currentState = currentState
	entity.eventsCounter = event.GetSequenceNumber()
	return nil
}

// sendErrorReply sends an error as a reply message
func (entity *EventSourcedActor) sendErrorReply(ctx *goakt.ReceiveContext, err error) {
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_ErrorReply{
			ErrorReply: &egopb.ErrorReply{
				Message: err.Error(),
			},
		},
	}

	ctx.Response(reply)
}

// getStateAndReply returns the current state of the entity
func (entity *EventSourcedActor) getStateAndReply(ctx *goakt.ReceiveContext) {
	latestEvent, err := entity.eventsStore.GetLatestEvent(ctx.Context(), entity.persistenceID)
	if err != nil {
		entity.sendErrorReply(ctx, err)
		return
	}

	resultingState := latestEvent.GetResultingState()
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.persistenceID,
				State:          resultingState,
				SequenceNumber: latestEvent.GetSequenceNumber(),
				Timestamp:      latestEvent.GetTimestamp(),
			},
		},
	}

	ctx.Response(reply)
}

// processCommandAndReply processes the incoming command
func (entity *EventSourcedActor) processCommandAndReply(ctx *goakt.ReceiveContext, command Command) {
	goCtx := ctx.Context()
	events, err := entity.behavior.HandleCommand(goCtx, command, entity.currentState)
	if err != nil {
		entity.sendErrorReply(ctx, err)
		return
	}

	// no-op when there are no events to process
	if len(events) == 0 {
		resultingState, _ := anypb.New(entity.currentState)
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  entity.persistenceID,
					State:          resultingState,
					SequenceNumber: entity.eventsCounter,
					Timestamp:      entity.lastCommandTime.Unix(),
				},
			},
		}
		ctx.Response(reply)
		return
	}

	shardNumber := ctx.ActorSystem().GetPartition(entity.persistenceID)
	topic := fmt.Sprintf(eventsTopic, shardNumber)

	var envelopes []*egopb.Event
	// process all events
	for _, event := range events {
		resultingState, err := entity.behavior.HandleEvent(goCtx, event, entity.currentState)
		if err != nil {
			entity.sendErrorReply(ctx, err)
			return
		}

		entity.eventsCounter++
		entity.currentState = resultingState
		entity.lastCommandTime = timestamppb.Now().AsTime()

		event, _ := anypb.New(event)
		state, _ := anypb.New(resultingState)

		envelope := &egopb.Event{
			PersistenceId:  entity.persistenceID,
			SequenceNumber: entity.eventsCounter,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      entity.lastCommandTime.Unix(),
			Shard:          uint64(shardNumber),
		}
		envelopes = append(envelopes, envelope)
	}

	ctx.Logger().Debugf("publishing events to topic: %s", topic)
	eg, goCtx := errgroup.WithContext(goCtx)
	eg.Go(func() error {
		for _, envelope := range envelopes {
			entity.eventsStream.Publish(topic, envelope)
		}
		return nil
	})

	eg.Go(func() error {
		return entity.eventsStore.WriteEvents(goCtx, envelopes)
	})

	if err := eg.Wait(); err != nil {
		entity.sendErrorReply(ctx, err)
		return
	}

	state, _ := anypb.New(entity.currentState)
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.persistenceID,
				State:          state,
				SequenceNumber: entity.eventsCounter,
				Timestamp:      entity.lastCommandTime.Unix(),
			},
		},
	}

	ctx.Response(reply)
}
