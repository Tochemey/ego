package ego

import (
	"context"

	"google.golang.org/protobuf/proto"
)

type Command proto.Message
type Event proto.Message
type State proto.Message

// EntityBehavior defines an event sourced behavior when modeling a CQRS EntityBehavior.
type EntityBehavior[T State] interface {
	// ID defines the id that will be used in the event journal.
	// This helps track the entity in the events store.
	ID() string
	// InitialState returns the event sourced actor initial state.
	// This is set as the initial state when there are no snapshots found the entity
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
