package ego

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
	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"google.golang.org/protobuf/proto"
)

// actor is an event sourced based actor
type actor struct {
	Behavior
	// specifies the events store
	eventsStore eventstore.EventsStore
	// specifies the current state
	currentState proto.Message

	eventsCounter   *atomic.Uint64
	lastCommandTime time.Time
	mu              sync.RWMutex
}

// ensure that the struct does implement the given interface
var _ actors.Actor = &actor{}

// newActor creates an instance of behaviorImpl provided the eventSourcedHandler and the events store
func newActor(behavior Behavior, eventsStore eventstore.EventsStore) *actor {
	// create an instance of entity and return it
	return &actor{
		eventsStore:   eventsStore,
		Behavior:      behavior,
		eventsCounter: atomic.NewUint64(0),
		mu:            sync.RWMutex{},
	}
}

// PreStart pre-starts the actor
// At this stage we connect to the various stores
func (x *actor) PreStart(ctx context.Context) error {
	// add a span context
	//ctx, span := telemetry.SpanContext(ctx, "PreStart")
	//defer span.End()
	// acquire the lock
	x.mu.Lock()
	// release lock when done
	defer x.mu.Unlock()

	// connect to the various stores
	if x.eventsStore == nil {
		return errors.New("events store is not defined")
	}

	// call the connect method of the journal store
	if err := x.eventsStore.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to the events store: %v", err)
	}

	// check whether there is a snapshot to recover from
	if err := x.recoverFromSnapshot(ctx); err != nil {
		return errors.Wrap(err, "failed to recover from snapshot")
	}
	return nil
}

// Receive processes any message dropped into the actor mailbox.
func (x *actor) Receive(ctx actors.ReceiveContext) {
	// add a span context
	_, span := telemetry.SpanContext(ctx.Context(), "Receive")
	defer span.End()

	// acquire the lock
	x.mu.Lock()
	// release lock when done
	defer x.mu.Unlock()

	// grab the command sent
	switch command := ctx.Message().(type) {
	case *egopb.GetStateCommand:
		x.getStateAndReply(ctx)
	default:
		x.processCommandAndReply(ctx, command)
	}
}

// PostStop prepares the actor to gracefully shutdown
func (x *actor) PostStop(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "PostStop")
	defer span.End()

	// acquire the lock
	x.mu.Lock()
	// release lock when done
	defer x.mu.Unlock()

	// disconnect the journal
	if err := x.eventsStore.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect the events store: %v", err)
	}
	return nil
}

// recoverFromSnapshot reset the persistent actor to the latest snapshot in case there is one
// this is vital when the entity actor is restarting.
func (x *actor) recoverFromSnapshot(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "RecoverFromSnapshot")
	defer span.End()

	// check whether there is a snapshot to recover from
	event, err := x.eventsStore.GetLatestEvent(ctx, x.ID())
	// handle the error
	if err != nil {
		return errors.Wrap(err, "failed to recover the latest journal")
	}

	// we do have the latest state just recover from it
	if event != nil {
		// set the current state
		if err := event.GetResultingState().UnmarshalTo(x.currentState); err != nil {
			return errors.Wrap(err, "failed unmarshal the latest state")
		}

		// set the event counter
		x.eventsCounter.Store(event.GetSequenceNumber())
		return nil
	}

	// in case there is no snapshot
	x.currentState = x.InitialState()
	return nil
}

// sendErrorReply sends an error as a reply message
func (x *actor) sendErrorReply(ctx actors.ReceiveContext, err error) {
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
func (x *actor) getStateAndReply(ctx actors.ReceiveContext) {
	// let us fetch the latest journal
	latestEvent, err := x.eventsStore.GetLatestEvent(ctx.Context(), x.ID())
	// handle the error
	if err != nil {
		x.sendErrorReply(ctx, err)
		return
	}

	// reply with the state unmarshalled
	resultingState := latestEvent.GetResultingState()
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  x.ID(),
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
func (x *actor) processCommandAndReply(ctx actors.ReceiveContext, command proto.Message) {
	// set the go context
	goCtx := ctx.Context()
	// pass the received command to the command handler
	event, err := x.HandleCommand(goCtx, command, x.currentState)
	// handle the command handler error
	if err != nil {
		// send an error reply
		x.sendErrorReply(ctx, err)
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
	resultingState, err := x.HandleEvent(goCtx, event, x.currentState)
	// handle the event handler error
	if err != nil {
		// send an error reply
		x.sendErrorReply(ctx, err)
		return
	}

	// increment the event counter
	x.eventsCounter.Inc()

	// set the current state for the next command
	x.currentState = resultingState

	// marshal the event and the resulting state
	marshaledEvent, _ := anypb.New(event)
	marshaledState, _ := anypb.New(resultingState)

	sequenceNumber := x.eventsCounter.Load()
	timestamp := timestamppb.Now()
	x.lastCommandTime = timestamp.AsTime()
	shardNumber := ctx.Self().ActorSystem().GetPartition(x.ID())

	// create the event
	envelope := &egopb.Event{
		PersistenceId:  x.ID(),
		SequenceNumber: sequenceNumber,
		IsDeleted:      false,
		Event:          marshaledEvent,
		ResultingState: marshaledState,
		Timestamp:      x.lastCommandTime.Unix(),
		Shard:          shardNumber,
	}

	// create a journal list
	journals := []*egopb.Event{envelope}

	// TODO persist the event in batch using a child actor
	if err := x.eventsStore.WriteEvents(goCtx, journals); err != nil {
		// send an error reply
		x.sendErrorReply(ctx, err)
		return
	}

	// create the command reply to send
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  x.ID(),
				State:          marshaledState,
				SequenceNumber: sequenceNumber,
				Timestamp:      x.lastCommandTime.Unix(),
			},
		},
	}

	// send the response
	ctx.Response(reply)
}
