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
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/eventstore"
	"github.com/tochemey/ego/eventstream"
	"github.com/tochemey/ego/internal/telemetry"
	"github.com/tochemey/goakt/actors"
	"github.com/tochemey/goakt/goaktpb"
)

var (
	eventsTopic = "topic.events.%d"
)

// actor is an event sourced based actor
type actor[T State] struct {
	EntityBehavior[T]
	// specifies the events store
	eventsStore eventstore.EventsStore
	// specifies the current state
	currentState T

	eventsCounter   *atomic.Uint64
	lastCommandTime time.Time
	mu              sync.RWMutex
	eventsStream    eventstream.Stream
}

// enforce compilation error
var _ actors.Actor = &actor[State]{}

// newActor creates an instance of actor provided the eventSourcedHandler and the events store
func newActor[T State](behavior EntityBehavior[T], eventsStore eventstore.EventsStore, eventsStream eventstream.Stream) *actor[T] {
	// create an instance of entity and return it
	return &actor[T]{
		eventsStore:    eventsStore,
		EntityBehavior: behavior,
		eventsCounter:  atomic.NewUint64(0),
		mu:             sync.RWMutex{},
		eventsStream:   eventsStream,
	}
}

// PreStart pre-starts the actor
// At this stage we connect to the various stores
func (entity *actor[T]) PreStart(ctx context.Context) error {
	spanCtx, span := telemetry.SpanContext(ctx, "PreStart")
	defer span.End()
	entity.mu.Lock()
	defer entity.mu.Unlock()

	if entity.eventsStore == nil {
		return errors.New("events store is not defined")
	}

	if err := entity.eventsStore.Ping(spanCtx); err != nil {
		return fmt.Errorf("failed to connect to the events store: %v", err)
	}

	return nil
}

// Receive processes any message dropped into the actor mailbox.
func (entity *actor[T]) Receive(ctx actors.ReceiveContext) {
	_, span := telemetry.SpanContext(ctx.Context(), "Receive")
	defer span.End()

	entity.mu.Lock()
	defer entity.mu.Unlock()

	// grab the command sent
	switch command := ctx.Message().(type) {
	case *goaktpb.PostStart:
		if err := entity.recoverFromSnapshot(ctx.Context()); err != nil {
			ctx.Err(errors.Wrap(err, "failed to recover from snapshot"))
		}
	case *egopb.GetStateCommand:
		entity.getStateAndReply(ctx)
	default:
		entity.processCommandAndReply(ctx, command)
	}
}

// PostStop prepares the actor to gracefully shutdown
func (entity *actor[T]) PostStop(ctx context.Context) error {
	_, span := telemetry.SpanContext(ctx, "PostStop")
	defer span.End()

	entity.mu.Lock()
	defer entity.mu.Unlock()

	return nil
}

// recoverFromSnapshot reset the persistent actor to the latest snapshot in case there is one
// this is vital when the entity actor is restarting.
func (entity *actor[T]) recoverFromSnapshot(ctx context.Context) error {
	spanCtx, span := telemetry.SpanContext(ctx, "RecoverFromSnapshot")
	defer span.End()

	event, err := entity.eventsStore.GetLatestEvent(spanCtx, entity.ID())
	if err != nil {
		return errors.Wrap(err, "failed to recover the latest journal")
	}

	// we do have the latest state just recover from it
	if event != nil {
		currentState := entity.InitialState()
		if err := event.GetResultingState().UnmarshalTo(currentState); err != nil {
			return errors.Wrap(err, "failed unmarshal the latest state")
		}
		entity.currentState = currentState

		entity.eventsCounter.Store(event.GetSequenceNumber())
		return nil
	}

	entity.currentState = entity.InitialState()
	return nil
}

// sendErrorReply sends an error as a reply message
func (entity *actor[T]) sendErrorReply(ctx actors.ReceiveContext, err error) {
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
func (entity *actor[T]) getStateAndReply(ctx actors.ReceiveContext) {
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
func (entity *actor[T]) processCommandAndReply(ctx actors.ReceiveContext, command Command) {
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
					SequenceNumber: entity.eventsCounter.Load(),
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

		entity.eventsCounter.Inc()
		entity.currentState = resultingState
		entity.lastCommandTime = timestamppb.Now().AsTime()

		event, _ := anypb.New(event)
		state, _ := anypb.New(resultingState)

		envelope := &egopb.Event{
			PersistenceId:  entity.ID(),
			SequenceNumber: entity.eventsCounter.Load(),
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      entity.lastCommandTime.Unix(),
			Shard:          shardNumber,
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
				SequenceNumber: entity.eventsCounter.Load(),
				Timestamp:      entity.lastCommandTime.Unix(),
			},
		},
	}

	ctx.Response(reply)
}
