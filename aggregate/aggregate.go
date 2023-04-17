package aggregate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tochemey/ego/storage"

	"github.com/pkg/errors"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/goakt/actors"
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
	// This helps track the aggregate in the events store.
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

// Aggregate is an event sourced based actor
type Aggregate[T State] struct {
	Behavior[T]
	// specifies the events store
	eventsStore storage.EventsStore
	// specifies the current state
	currentState T

	eventsCounter   *atomic.Uint64
	lastCommandTime time.Time
	mu              sync.RWMutex
}

// enforce compilation error
var _ actors.Actor = &Aggregate[State]{}

// New creates an instance of Aggregate provided the eventSourcedHandler and the events store
func New[T State](behavior Behavior[T], eventsStore storage.EventsStore) *Aggregate[T] {
	// create an instance of aggregate and return it
	return &Aggregate[T]{
		eventsStore:   eventsStore,
		Behavior:      behavior,
		eventsCounter: atomic.NewUint64(0),
		mu:            sync.RWMutex{},
	}
}

// PreStart pre-starts the actor
// At this stage we connect to the various stores
func (a *Aggregate[T]) PreStart(ctx context.Context) error {
	// add a span context
	//ctx, span := telemetry.SpanContext(ctx, "PreStart")
	//defer span.End()
	// acquire the lock
	a.mu.Lock()
	// release lock when done
	defer a.mu.Unlock()

	// connect to the various stores
	if a.eventsStore == nil {
		return errors.New("events store is not defined")
	}

	// call the connect method of the journal store
	if err := a.eventsStore.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to the events store: %v", err)
	}

	// check whether there is a snapshot to recover from
	if err := a.recoverFromSnapshot(ctx); err != nil {
		return errors.Wrap(err, "failed to recover from snapshot")
	}
	return nil
}

// Receive processes any message dropped into the actor mailbox.
func (a *Aggregate[T]) Receive(ctx actors.ReceiveContext) {
	// grab the context
	goCtx := ctx.Context()
	// add a span context
	//goCtx, span := telemetry.SpanContext(ctx.Context(), "Receive")
	//defer span.End()

	// acquire the lock
	a.mu.Lock()
	// release lock when done
	defer a.mu.Unlock()

	// grab the command sent
	switch command := ctx.Message().(type) {
	case *egopb.GetStateCommand:
		// let us fetch the latest journal
		latestEvent, err := a.eventsStore.GetLatestEvent(goCtx, a.ID())
		// handle the error
		if err != nil {
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
			return
		}

		// reply with the state unmarshalled
		resultingState := latestEvent.GetResultingState()
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  a.ID(),
					State:          resultingState,
					SequenceNumber: latestEvent.GetSequenceNumber(),
					Timestamp:      latestEvent.GetTimestamp(),
				},
			},
		}

		// send the response
		ctx.Response(reply)
	default:
		// pass the received command to the command handler
		event, err := a.HandleCommand(goCtx, command, a.currentState)
		// handle the command handler error
		if err != nil {
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
		resultingState, err := a.HandleEvent(goCtx, event, a.currentState)
		// handle the event handler error
		if err != nil {
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
			return
		}

		// increment the event counter
		a.eventsCounter.Inc()

		// set the current state for the next command
		a.currentState = resultingState

		// marshal the event and the resulting state
		marshaledEvent, _ := anypb.New(event)
		marshaledState, _ := anypb.New(resultingState)

		sequenceNumber := a.eventsCounter.Load()
		timestamp := timestamppb.Now()
		a.lastCommandTime = timestamp.AsTime()

		// create the event
		envelope := &egopb.Event{
			PersistenceId:  a.ID(),
			SequenceNumber: sequenceNumber,
			IsDeleted:      false,
			Event:          marshaledEvent,
			ResultingState: marshaledState,
			Timestamp:      a.lastCommandTime.Unix(),
		}

		// create a journal list
		journals := []*egopb.Event{envelope}

		// TODO persist the event in batch using a child actor
		if err := a.eventsStore.WriteEvents(goCtx, journals); err != nil {
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
			return
		}

		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  a.ID(),
					State:          marshaledState,
					SequenceNumber: sequenceNumber,
					Timestamp:      a.lastCommandTime.Unix(),
				},
			},
		}

		// send the response
		ctx.Response(reply)
	}
}

// PostStop prepares the actor to gracefully shutdown
func (a *Aggregate[T]) PostStop(ctx context.Context) error {
	// add a span context
	//ctx, span := telemetry.SpanContext(ctx, "PostStop")
	//defer span.End()

	// acquire the lock
	a.mu.Lock()
	// release lock when done
	defer a.mu.Unlock()

	// disconnect the journal
	if err := a.eventsStore.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect the events store: %v", err)
	}
	return nil
}

// recoverFromSnapshot reset the persistent actor to the latest snapshot in case there is one
// this is vital when the aggregate actor is restarting.
func (a *Aggregate[T]) recoverFromSnapshot(ctx context.Context) error {
	// add a span context
	//ctx, span := telemetry.SpanContext(ctx, "RecoverFromSnapshot")
	//defer span.End()

	// check whether there is a snapshot to recover from
	event, err := a.eventsStore.GetLatestEvent(ctx, a.ID())
	// handle the error
	if err != nil {
		return errors.Wrap(err, "failed to recover the latest journal")
	}

	// we do have the latest state just recover from it
	if event != nil {
		// set the current state
		if err := event.GetResultingState().UnmarshalTo(a.currentState); err != nil {
			return errors.Wrap(err, "failed unmarshal the latest state")
		}

		// set the event counter
		a.eventsCounter.Store(event.GetSequenceNumber())
		return nil
	}

	// in case there is no snapshot
	a.currentState = a.InitialState()
	return nil
}
