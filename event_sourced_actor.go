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
	"errors"
	"fmt"
	"time"

	"github.com/tochemey/goakt/v2/actors"
	"github.com/tochemey/goakt/v2/goaktpb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/persistence"
)

var (
	eventsTopic = "topic.events.%d"
)

// eventSourcedActor is an event sourced based actor
type eventSourcedActor struct {
	EventSourcedBehavior
	eventsStore     persistence.EventsStore
	currentState    State
	eventsCounter   uint64
	lastCommandTime time.Time
	eventsStream    eventstream.Stream
}

// implements the actors.Actor interface
var _ actors.Actor = (*eventSourcedActor)(nil)

// newEventSourcedActor creates an instance of actor provided the eventSourcedHandler and the events store
func newEventSourcedActor(behavior EventSourcedBehavior, eventsStore persistence.EventsStore, eventsStream eventstream.Stream) *eventSourcedActor {
	return &eventSourcedActor{
		eventsStore:          eventsStore,
		EventSourcedBehavior: behavior,
		eventsStream:         eventsStream,
	}
}

// PreStart pre-starts the actor
func (entity *eventSourcedActor) PreStart(ctx context.Context) error {
	if entity.eventsStore == nil {
		return errors.New("events store is not defined")
	}

	if err := entity.eventsStore.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to the events store: %w", err)
	}

	return entity.recoverFromSnapshot(ctx)
}

// Receive processes any message dropped into the actor mailbox.
func (entity *eventSourcedActor) Receive(ctx *actors.ReceiveContext) {
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
func (entity *eventSourcedActor) PostStop(ctx context.Context) error {
	entity.eventsCounter = 0
	return nil
}

// recoverFromSnapshot reset the persistent actor to the latest snapshot in case there is one
// this is vital when the entity actor is restarting.
func (entity *eventSourcedActor) recoverFromSnapshot(ctx context.Context) error {
	event, err := entity.eventsStore.GetLatestEvent(ctx, entity.ID())
	if err != nil {
		return fmt.Errorf("failed to recover the latest journal: %w", err)
	}

	// we do have the latest state just recover from it
	if event != nil {
		currentState := entity.InitialState()
		if err := event.GetResultingState().UnmarshalTo(currentState); err != nil {
			return fmt.Errorf("failed unmarshal the latest state: %w", err)
		}
		entity.currentState = currentState
		entity.eventsCounter = event.GetSequenceNumber()
		return nil
	}

	entity.currentState = entity.InitialState()
	return nil
}

// sendErrorReply sends an error as a reply message
func (entity *eventSourcedActor) sendErrorReply(ctx *actors.ReceiveContext, err error) {
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
func (entity *eventSourcedActor) getStateAndReply(ctx *actors.ReceiveContext) {
	latestEvent, err := entity.eventsStore.GetLatestEvent(ctx.Context(), entity.ID())
	if err != nil {
		entity.sendErrorReply(ctx, err)
		return
	}

	resultingState := latestEvent.GetResultingState()
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.ID(),
				State:          resultingState,
				SequenceNumber: latestEvent.GetSequenceNumber(),
				Timestamp:      latestEvent.GetTimestamp(),
			},
		},
	}

	ctx.Response(reply)
}

// processCommandAndReply processes the incoming command
func (entity *eventSourcedActor) processCommandAndReply(ctx *actors.ReceiveContext, command Command) {
	goCtx := ctx.Context()
	events, err := entity.HandleCommand(goCtx, command, entity.currentState)
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
					PersistenceId:  entity.ID(),
					State:          resultingState,
					SequenceNumber: entity.eventsCounter,
					Timestamp:      entity.lastCommandTime.Unix(),
				},
			},
		}
		ctx.Response(reply)
		return
	}

	shardNumber := ctx.Self().ActorSystem().GetPartition(entity.ID())
	topic := fmt.Sprintf(eventsTopic, shardNumber)

	var envelopes []*egopb.Event
	// process all events
	for _, event := range events {
		resultingState, err := entity.HandleEvent(goCtx, event, entity.currentState)
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
			PersistenceId:  entity.ID(),
			SequenceNumber: entity.eventsCounter,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      entity.lastCommandTime.Unix(),
			Shard:          uint64(shardNumber),
		}
		envelopes = append(envelopes, envelope)
	}

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
				PersistenceId:  entity.ID(),
				State:          state,
				SequenceNumber: entity.eventsCounter,
				Timestamp:      entity.lastCommandTime.Unix(),
			},
		},
	}

	ctx.Response(reply)
}
