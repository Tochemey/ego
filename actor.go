/*
 * Copyright (c) 2022-2023 Tochemey
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
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/eventstore"
	"github.com/tochemey/ego/eventstream"
	"github.com/tochemey/ego/internal/telemetry"
	"github.com/tochemey/goakt/actors"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "PreStart")
	defer span.End()
	// acquire the lock
	entity.mu.Lock()
	// release lock when done
	defer entity.mu.Unlock()

	// connect to the various stores
	if entity.eventsStore == nil {
		return errors.New("events store is not defined")
	}

	// call the connect method of the journal store
	if err := entity.eventsStore.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to the events store: %v", err)
	}

	// check whether there is a snapshot to recover from
	if err := entity.recoverFromSnapshot(ctx); err != nil {
		return errors.Wrap(err, "failed to recover from snapshot")
	}
	return nil
}

// Receive processes any message dropped into the actor mailbox.
func (entity *actor[T]) Receive(ctx actors.ReceiveContext) {
	// add a span context
	_, span := telemetry.SpanContext(ctx.Context(), "Receive")
	defer span.End()

	// acquire the lock
	entity.mu.Lock()
	// release lock when done
	defer entity.mu.Unlock()

	// grab the command sent
	switch command := ctx.Message().(type) {
	case *egopb.GetStateCommand:
		entity.getStateAndReply(ctx)
	default:
		entity.processCommandAndReply(ctx, command)
	}
}

// PostStop prepares the actor to gracefully shutdown
func (entity *actor[T]) PostStop(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "PostStop")
	defer span.End()

	// acquire the lock
	entity.mu.Lock()
	// release lock when done
	defer entity.mu.Unlock()

	return nil
}

// recoverFromSnapshot reset the persistent actor to the latest snapshot in case there is one
// this is vital when the entity actor is restarting.
func (entity *actor[T]) recoverFromSnapshot(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "RecoverFromSnapshot")
	defer span.End()

	// check whether there is a snapshot to recover from
	event, err := entity.eventsStore.GetLatestEvent(ctx, entity.ID())
	// handle the error
	if err != nil {
		return errors.Wrap(err, "failed to recover the latest journal")
	}

	// we do have the latest state just recover from it
	if event != nil {
		// set the current state
		if err := event.GetResultingState().UnmarshalTo(entity.currentState); err != nil {
			return errors.Wrap(err, "failed unmarshal the latest state")
		}

		// set the event counter
		entity.eventsCounter.Store(event.GetSequenceNumber())
		return nil
	}

	// in case there is no snapshot
	entity.currentState = entity.InitialState()
	return nil
}

// sendErrorReply sends an error as a reply message
func (entity *actor[T]) sendErrorReply(ctx actors.ReceiveContext, err error) {
	// create a new error reply
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_ErrorReply{
			ErrorReply: &egopb.ErrorReply{
				Message: err.Error(),
			},
		},
	}
	// send the response
	ctx.Response(reply)
}

// getStateAndReply returns the current state of the entity
func (entity *actor[T]) getStateAndReply(ctx actors.ReceiveContext) {
	// let us fetch the latest journal
	latestEvent, err := entity.eventsStore.GetLatestEvent(ctx.Context(), entity.ID())
	// handle the error
	if err != nil {
		entity.sendErrorReply(ctx, err)
		return
	}

	// reply with the state unmarshalled
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

	// send the response
	ctx.Response(reply)
}

// processCommandAndReply processes the incoming command
func (entity *actor[T]) processCommandAndReply(ctx actors.ReceiveContext, command Command) {
	// set the go context
	goCtx := ctx.Context()
	// pass the received command to the command handler
	event, err := entity.HandleCommand(goCtx, command, entity.currentState)
	// handle the command handler error
	if err != nil {
		// send an error reply
		entity.sendErrorReply(ctx, err)
		return
	}

	// if the event is nil nothing is persisted, and we return no reply
	if event == nil {
		// get the current state and marshal it
		resultingState, _ := anypb.New(entity.currentState)
		// create the command reply to send out
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
		// send the response
		ctx.Response(reply)
		return
	}

	// process the event by calling the event handler
	resultingState, err := entity.HandleEvent(goCtx, event, entity.currentState)
	// handle the event handler error
	if err != nil {
		// send an error reply
		entity.sendErrorReply(ctx, err)
		return
	}

	// increment the event counter
	entity.eventsCounter.Inc()

	// set the current state for the next command
	entity.currentState = resultingState

	// marshal the event and the resulting state
	marshaledEvent, _ := anypb.New(event)
	marshaledState, _ := anypb.New(resultingState)

	sequenceNumber := entity.eventsCounter.Load()
	timestamp := timestamppb.Now()
	entity.lastCommandTime = timestamp.AsTime()
	shardNumber := ctx.Self().ActorSystem().GetPartition(entity.ID())

	// create the event
	envelope := &egopb.Event{
		PersistenceId:  entity.ID(),
		SequenceNumber: sequenceNumber,
		IsDeleted:      false,
		Event:          marshaledEvent,
		ResultingState: marshaledState,
		Timestamp:      entity.lastCommandTime.Unix(),
		Shard:          shardNumber,
	}

	// create a journal list
	journals := []*egopb.Event{envelope}

	// define the topic for the given shard
	topic := fmt.Sprintf(eventsTopic, shardNumber)

	// publish to the event stream and persist the event to the events store
	eg, goCtx := errgroup.WithContext(goCtx)

	// publish the message to the topic
	eg.Go(func() error {
		entity.eventsStream.Publish(topic, envelope)
		return nil
	})

	// persist the event to the events store
	eg.Go(func() error {
		return entity.eventsStore.WriteEvents(goCtx, journals)
	})

	// handle the persistence error
	if err := eg.Wait(); err != nil {
		// send an error reply
		entity.sendErrorReply(ctx, err)
		return
	}

	// create the command reply to send
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.ID(),
				State:          marshaledState,
				SequenceNumber: sequenceNumber,
				Timestamp:      entity.lastCommandTime.Unix(),
			},
		},
	}

	// send the response
	ctx.Response(reply)
}
