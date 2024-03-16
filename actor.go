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
	spanCtx, span := telemetry.SpanContext(ctx, "PreStart")
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
	if err := entity.eventsStore.Ping(spanCtx); err != nil {
		return fmt.Errorf("failed to connect to the events store: %v", err)
	}

	// check whether there is a snapshot to recover from
	if err := entity.recoverFromSnapshot(spanCtx); err != nil {
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
	_, span := telemetry.SpanContext(ctx, "PostStop")
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
	spanCtx, span := telemetry.SpanContext(ctx, "RecoverFromSnapshot")
	defer span.End()

	// check whether there is a snapshot to recover from
	event, err := entity.eventsStore.GetLatestEvent(spanCtx, entity.ID())
	// handle the error
	if err != nil {
		return errors.Wrap(err, "failed to recover the latest journal")
	}

	// we do have the latest state just recover from it
	if event != nil {
		// set the current state
		currentState := entity.InitialState()
		if err := event.GetResultingState().UnmarshalTo(currentState); err != nil {
			return errors.Wrap(err, "failed unmarshal the latest state")
		}
		entity.currentState = currentState

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

	envelopesChan := make(chan *egopb.Event, 1)
	eg, goCtx := errgroup.WithContext(goCtx)
	shardNumber := ctx.Self().ActorSystem().GetPartition(entity.ID())
	topic := fmt.Sprintf(eventsTopic, shardNumber)

	// 1-> process each event
	// 2-> increment the events (processed) counter of the entity
	// 3-> set the current state of the entity to the resulting state
	// 4-> set the latCommandTime of the entity with the current timestamp
	// 5-> marshal the resulting state and event to build the persistence envelope
	// 6-> push the envelope to the envelope channel for downstream processing
	eg.Go(func() error {
		defer close(envelopesChan)
		for _, event := range events {
			resultingState, err := entity.HandleEvent(goCtx, event, entity.currentState)
			if err != nil {
				entity.sendErrorReply(ctx, err)
				return err
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

			select {
			case envelopesChan <- envelope:
			case <-goCtx.Done():
				return goCtx.Err()
			}
		}
		return nil
	})

	// 1-> publish the processed event
	// 2-> build the list of envelopes for persistence
	// 3-> persist the batch of envelopes when the channel is closed and return
	eg.Go(func() error {
		var envelopes []*egopb.Event
		for {
			select {
			case envelope, ok := <-envelopesChan:
				// channel closed, persist the envelopes
				if !ok {
					return entity.eventsStore.WriteEvents(goCtx, envelopes)
				}

				entity.eventsStream.Publish(topic, envelope)
				envelopes = append(envelopes, envelope)
			case <-goCtx.Done():
				return goCtx.Err()
			}
		}
	})

	// wait for all go routines to complete
	if err := eg.Wait(); err != nil {
		entity.sendErrorReply(ctx, err)
		return
	}

	state, _ := anypb.New(entity.currentState)

	// create the command reply to send
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

	// send the response
	ctx.Response(reply)
}
