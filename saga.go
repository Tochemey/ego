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
	"time"

	"github.com/tochemey/goakt/v4/extension"
	"google.golang.org/protobuf/proto"
)

// SagaBehavior defines a long-running business process that coordinates
// multiple entities. Sagas react to events, send commands to entities,
// and manage compensation logic for rollback on failures.
//
// A saga is itself event-sourced: it persists its own events to track
// which steps have completed, enabling recovery after restarts.
type SagaBehavior interface {
	extension.Dependency
	// ID returns the unique identifier for this saga instance.
	ID() string
	// InitialState returns the saga's initial state.
	InitialState() State
	// HandleEvent is called when an event from the event stream matches
	// this saga's interest. It returns the saga's reaction: commands to
	// send to other entities, events to persist for the saga's own state,
	// and/or signals to complete or compensate.
	HandleEvent(ctx context.Context, event Event, state State) (*SagaAction, error)
	// HandleResult is called when a command sent to an entity returns a result.
	// This allows the saga to react to entity responses.
	HandleResult(ctx context.Context, entityID string, result State, sagaState State) (*SagaAction, error)
	// HandleError is called when a command sent to an entity fails.
	// The saga can decide to compensate or retry.
	HandleError(ctx context.Context, entityID string, err error, sagaState State) (*SagaAction, error)
	// ApplyEvent applies a saga event to update the saga's internal state.
	// This must be a pure function for replay correctness.
	ApplyEvent(ctx context.Context, event Event, state State) (State, error)
	// Compensate is called when the saga needs to roll back completed steps.
	// It receives the current saga state and returns commands to undo prior work.
	Compensate(ctx context.Context, state State) ([]SagaCommand, error)
}

// SagaAction describes what the saga should do next after processing an event or result.
type SagaAction struct {
	// Commands to send to other entities.
	Commands []SagaCommand
	// Events to persist for this saga's own state.
	Events []Event
	// Complete marks the saga as finished successfully.
	Complete bool
	// Compensate triggers compensation of all completed steps.
	Compensate bool
}

// SagaCommand represents a command to send to another entity.
type SagaCommand struct {
	// EntityID is the target entity's persistence ID.
	EntityID string
	// Command is the command to send.
	Command Command
	// Timeout is the maximum time to wait for a response.
	Timeout time.Duration
}

// SagaStatus represents the current status of a saga.
type SagaStatus int

const (
	// SagaRunning indicates the saga is actively processing.
	SagaRunning SagaStatus = iota
	// SagaCompleted indicates the saga finished successfully.
	SagaCompleted
	// SagaCompensating indicates the saga is rolling back.
	SagaCompensating
	// SagaFailed indicates the saga failed and compensation also failed.
	SagaFailed
)

// String returns the string representation of the saga status.
func (s SagaStatus) String() string {
	switch s {
	case SagaRunning:
		return "running"
	case SagaCompleted:
		return "completed"
	case SagaCompensating:
		return "compensating"
	case SagaFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// SagaInfo holds runtime information about a saga.
type SagaInfo struct {
	// ID is the saga's unique identifier.
	ID string
	// Status is the saga's current status.
	Status SagaStatus
	// State is the saga's current state (may be nil if not started).
	State proto.Message
}
