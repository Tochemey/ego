package entity

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/eventstore"
	"github.com/tochemey/ego/internal/telemetry"
	"github.com/tochemey/goakt/actors"
	goaktmessagesv1 "github.com/tochemey/goakt/messages/v1"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Command proto.Message
type Event proto.Message
type State proto.Message

// Behavior defines an event sourced behavior when modeling a CQRS Behavior.
type Behavior[T State] interface {
	// ID defines the id that will be used in the event journal.
	// This helps track the entity in the events store.
	ID() string
	// InitialState returns the event sourced actor initial state
	InitialState() T
	// HandleCommand helps handle commands received by the event sourced actor. The command handlers define how to handle each incoming command,
	// which validations must be applied, and finally, which events will be persisted if any. When there is no event to be persisted a nil can
	// be returned as a no-op. Command handlers are the meat of the event sourced actor.
	// They encode the business rules of your event sourced actor and act as a guardian of the event sourced actor consistency.
	// The command eventSourcedHandler must first validate that the incoming command can be applied to the current model state.
	//  Any decision should be solely based on the data passed in the commands and the state of the Behavior.
	// In case of successful validation, one or more events expressing the mutations are persisted.
	// Once the events are persisted, they are applied to the state producing a new valid state.
	HandleCommand(ctx context.Context, command Command, priorState T) (event Event, err error)
	// HandleEvent handle events emitted by the command handlers. The event handlers are used to mutate the state of the event sourced actor by applying the events to it.
	// Event handlers must be pure functions as they will be used when instantiating the event sourced actor and replaying the event journal.
	HandleEvent(ctx context.Context, event Event, priorState T) (state T, err error)
}

// Entity is an event sourced based actor
type Entity[T State] struct {
	Behavior[T]
	// specifies the events store
	eventsStore eventstore.EventsStore
	// specifies the current state
	currentState T

	eventsCounter   *atomic.Uint64
	lastCommandTime time.Time
	mu              sync.RWMutex
}

// enforce compilation error
var _ actors.Actor = &Entity[State]{}

// New creates an instance of Entity provided the eventSourcedHandler and the events store
func New[T State](behavior Behavior[T], eventsStore eventstore.EventsStore) *Entity[T] {
	// create an instance of entity and return it
	return &Entity[T]{
		eventsStore:   eventsStore,
		Behavior:      behavior,
		eventsCounter: atomic.NewUint64(0),
		mu:            sync.RWMutex{},
	}
}

// PreStart pre-starts the actor
// At this stage we connect to the various stores
func (entity *Entity[T]) PreStart(ctx context.Context) error {
	// add a span context
	//ctx, span := telemetry.SpanContext(ctx, "PreStart")
	//defer span.End()
	// acquire the lock
	entity.mu.Lock()
	// release lock when done
	defer entity.mu.Unlock()

	// connect to the various stores
	if entity.eventsStore == nil {
		return errors.New("events store is not defined")
	}

	// call the connect method of the journal store
	if err := entity.eventsStore.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to the events store: %v", err)
	}

	// check whether there is a snapshot to recover from
	if err := entity.recoverFromSnapshot(ctx); err != nil {
		return errors.Wrap(err, "failed to recover from snapshot")
	}
	return nil
}

// Receive processes any message dropped into the actor mailbox.
func (entity *Entity[T]) Receive(ctx actors.ReceiveContext) {
	// add a span context
	_, span := telemetry.SpanContext(ctx.Context(), "Receive")
	defer span.End()

	// acquire the lock
	entity.mu.Lock()
	// release lock when done
	defer entity.mu.Unlock()

	// grab the command sent
	switch command := ctx.Message().(type) {
	case *goaktmessagesv1.RemoteMessage:
		// this will help handle messages when cluster mode is enabled
		msg := command.GetMessage()
		// let us unpack the message
		unpacked, err := msg.UnmarshalNew()
		// handle the error
		if err != nil {
			entity.sendErrorReply(ctx, err)
			return
		}
		switch unpacked.(type) {
		case *egopb.GetStateCommand:
			entity.getStateAndReply(ctx)
		default:
			entity.processCommandAndReply(ctx, unpacked)
		}
	case *egopb.GetStateCommand:
		entity.getStateAndReply(ctx)
	default:
		entity.processCommandAndReply(ctx, command)
	}
}

// PostStop prepares the actor to gracefully shutdown
func (entity *Entity[T]) PostStop(ctx context.Context) error {
	// add a span context
	//ctx, span := telemetry.SpanContext(ctx, "PostStop")
	//defer span.End()

	// acquire the lock
	entity.mu.Lock()
	// release lock when done
	defer entity.mu.Unlock()

	// disconnect the journal
	if err := entity.eventsStore.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect the events store: %v", err)
	}
	return nil
}

// recoverFromSnapshot reset the persistent actor to the latest snapshot in case there is one
// this is vital when the entity actor is restarting.
func (entity *Entity[T]) recoverFromSnapshot(ctx context.Context) error {
	// add a span context
	//ctx, span := telemetry.SpanContext(ctx, "RecoverFromSnapshot")
	//defer span.End()

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
func (entity *Entity[T]) sendErrorReply(ctx actors.ReceiveContext, err error) {
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
func (entity *Entity[T]) getStateAndReply(ctx actors.ReceiveContext) {
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
func (entity *Entity[T]) processCommandAndReply(ctx actors.ReceiveContext, command Command) {
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
		// create a new error reply
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_NoReply{
				NoReply: &egopb.NoReply{},
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
	shardNumber := ctx.Self().ActorSystem().GetPartition(goCtx, entity.ID())

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

	// TODO persist the event in batch using a child actor
	if err := entity.eventsStore.WriteEvents(goCtx, journals); err != nil {
		// send an error reply
		entity.sendErrorReply(ctx, err)
		return
	}

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
