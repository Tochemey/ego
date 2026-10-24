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

package projection

import (
	"errors"
	"time"
)

var (
	// ErrHandlerRequired is returned when a projection starts with Options that
	// set neither Handler nor TransactionalHandler.
	ErrHandlerRequired = errors.New("projection handler is required: set Options.Handler or Options.TransactionalHandler")
	// ErrConflictingHandlers is returned when a projection starts with Options
	// that set both Handler and TransactionalHandler.
	ErrConflictingHandlers = errors.New("projection handlers conflict: set either Options.Handler or Options.TransactionalHandler, not both")
)

// Options holds the configuration required to run a projection handler.
type Options struct {
	// Handler defines how events are processed by the projection, with
	// at-least-once delivery. Set either Handler or TransactionalHandler.
	Handler Handler
	// TransactionalHandler processes the events and records their offsets in
	// its own database transaction, for exactly-once effects in that database.
	// Set either Handler or TransactionalHandler.
	TransactionalHandler TransactionalHandler
	// BufferSize controls how many events to buffer in memory before invoking the handler.
	// Zero or less uses the default of 500 events.
	BufferSize int
	// StartOffset is the point in time from which to begin processing events.
	StartOffset time.Time
	// ResetOffset is the fallback offset used when the projection needs to recover or replay events.
	ResetOffset time.Time
	// PullInterval defines how frequently the projection polls the event store for new events.
	// Zero or less uses the default of one second.
	PullInterval time.Duration
	// Recovery configures how the projection behaves when the handler fails to process an event.
	// When nil, a default recovery strategy is applied.
	Recovery *Recovery
	// DeadLetterHandler receives events that the projection handler failed to process
	// after exhausting its recovery policy. When nil, failed events are silently discarded.
	DeadLetterHandler DeadLetterHandler
}

// Validate reports whether the options set exactly one of Handler and
// TransactionalHandler.
func (o *Options) Validate() error {
	switch {
	case o.Handler == nil && o.TransactionalHandler == nil:
		return ErrHandlerRequired
	case o.Handler != nil && o.TransactionalHandler != nil:
		return ErrConflictingHandlers
	default:
		return nil
	}
}
