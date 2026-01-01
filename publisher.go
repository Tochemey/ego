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
	"errors"

	"github.com/tochemey/ego/v3/egopb"
)

// ErrPublisherNotStarted is returned when an operation is attempted on a publisher that has not been started.
var ErrPublisherNotStarted = errors.New("publisher not started")

// EventPublisher defines an interface for publishing events.
// Implementations of this interface are responsible for delivering events
// to the appropriate event stream, message broker, or event log.
type EventPublisher interface {
	// ID returns the unique identifier for the event publisher.
	// This identifier is used to distinguish between different publishers.
	//
	// Returns:
	//   - string: The unique identifier for the event publisher.
	ID() string

	// Publish delivers an event to the event stream or message broker.
	// It requires a context for cancellation and timeouts, and the event
	// to be published. Implementations of this method are responsible for
	// ensuring that the event is successfully delivered.
	//
	// Parameters:
	//   - ctx: The context for managing cancellation and timeouts.
	//   - event: The event to be published.
	//
	// Returns:
	//   - error: If an error occurs during event publication, it is returned.
	Publish(ctx context.Context, event *egopb.Event) error

	// Close closes the event publisher.
	// This method is called when the publisher is being shut down.
	// It should be used to clean up any resources used by the publisher.
	//
	// Parameters:
	//   - ctx: The context for managing cancellation and timeouts.
	//
	// Returns:
	//   - error: If an error occurs during close, it is returned.
	Close(context.Context) error
}

// StatePublisher defines an interface for publishing durable state changes.
// This is typically used to persist entity state updates in a distributed system.
type StatePublisher interface {
	// ID returns the unique identifier for the state publisher.
	// This identifier is used to distinguish between different publishers.
	//
	// Returns:
	//   - string: The unique identifier for the state publisher.
	ID() string

	// Publish delivers a durable state update to the state store.
	// The provided state will be persisted and should be used to
	// ensure consistency across nodes in a distributed system.
	//
	// Parameters:
	//   - ctx: The context for managing cancellation and timeouts.
	//   - state: The durable state to be published.
	//
	// Returns:
	//   - error: If an error occurs during state publication, it is returned.
	Publish(ctx context.Context, state *egopb.DurableState) error

	// Close closes the durbable state publisher.
	// This method is called when the publisher is being shut down.
	// It should be used to clean up any resources used by the publisher.
	//
	// Parameters:
	//   - ctx: The context for managing cancellation and timeouts.
	//
	// Returns:
	//   - error: If an error occurs during close, it is returned.
	Close(context.Context) error
}
