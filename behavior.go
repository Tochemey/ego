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

	"github.com/tochemey/goakt/v3/extension"
	"google.golang.org/protobuf/proto"
)

type Command proto.Message
type Event proto.Message
type State proto.Message

// EventSourcedBehavior defines an event-sourced behavior when modeling a CQRS EventSourcedBehavior.
type EventSourcedBehavior interface {
	extension.Dependency
	// InitialState returns the event sourced actor initial state.
	// This is set as the initial state when there are no snapshots found the entity
	InitialState() State
	// HandleCommand helps handle commands received by the event sourced actor. The command handlers define how to handle each incoming command,
	// which validations must be applied, and finally, which events will be persisted if any. When there is no event to be persisted a nil can
	// be returned as a no-op. Command handlers are the meat of the event sourced actor.
	// They encode the business rules of your event sourced actor and act as a guardian of the event sourced actor consistency.
	// The command must first validate that the incoming command can be applied to the current model state.
	//  Any decision should be solely based on the data passed in the commands and the state of the Behavior.
	// In case of successful validation, one or more events expressing the mutations are persisted.
	// Once the events are persisted, they are applied to the state producing a new valid state.
	// Every event emitted are processed one after the other in the same order they were emitted to guarantee consistency.
	// It is at the discretion of the application developer to know in which order a given command should return the list of events
	// This is really powerful when a command needs to return two events. For instance, an OpenAccount command can result in two events: one is AccountOpened and the second is AccountCredited
	HandleCommand(ctx context.Context, command Command, priorState State) (events []Event, err error)
	// HandleEvent handle events emitted by the command handlers. The event handlers are used to mutate the state of the event sourced actor by applying the events to it.
	// Event handlers must be pure functions as they will be used when instantiating the event sourced actor and replaying the event journal.
	HandleEvent(ctx context.Context, event Event, priorState State) (state State, err error)
}

// DurableStateBehavior represents a type of Actor that persists its full state after processing each command instead of using event sourcing.
// This type of Actor keeps its current state in memory during command handling and based upon the command response
// persists its full state into a durable store. The store can be a SQL or NoSQL database.
// The whole concept is given the current state of the actor and a command produce a new state with a higher version as shown in this diagram: (State, Command) => State
// DurableStateBehavior reacts to commands which result in a new version of the actor state. Only the latest version of the actor state is
// persisted to the durable store. There is no concept of history regarding the actor state since this is not an event sourced actor.
// However, one can rely on the version number of the actor state and exactly know how the actor state has evolved overtime.
// State actor version number are numerically incremented by the command handler which means it is imperative that the newer version of the state is greater than the current version by one.
//
// DurableStateBehavior will attempt to recover its state whenever available from the durable state.
// During a normal shutdown process, it will persist its current state to the durable store prior to shutting down.
// This behavior help maintain some consistency across the actor state evolution.
type DurableStateBehavior interface {
	extension.Dependency
	// InitialState returns the durable state actor initial state.
	// This is set as the initial state when there are no snapshots found the entity
	InitialState() State
	// HandleCommand processes every command sent to the DurableStateBehavior. One needs to use the command, the priorVersion and the priorState sent to produce a newState and newVersion.
	// This defines how to handle each incoming command, which validations must be applied, and finally, whether a resulting state will be persisted depending upon the response.
	// They encode the business rules of your durable state actor and act as a guardian of the actor consistency.
	// The command handler must first validate that the incoming command can be applied to the current model state.
	// Any decision should be solely based on the data passed in the command, the priorVersion and the priorState.
	// In case of successful validation and processing , the new state will be stored in the durable store depending upon response.
	// The actor state will be updated with the newState only if the newVersion is 1 more than the already existing state.
	HandleCommand(ctx context.Context, command Command, priorVersion uint64, priorState State) (newState State, newVersion uint64, err error)
}
