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
	goakt "github.com/tochemey/goakt/v4/actor"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/persistence"
)

// persistEventsRequest is sent from the [EventSourcedActor] to the
// [eventsWriterActor] to persist a batch of event envelopes and publish them
// to the event stream.
type persistEventsRequest struct {
	envelopes []*egopb.Event
	topic     string
}

// persistEventsResponse is sent from the [eventsWriterActor] back to the
// [EventSourcedActor] after an attempt to persist events. A nil Err indicates
// success; a non-nil Err carries the store write failure so the parent can
// decide whether to stop itself.
type persistEventsResponse struct {
	Err error
}

// eventsWriterActor persists events to the events store and publishes them to
// the event stream. Events are published only after the store write succeeds,
// ensuring that downstream consumers never observe events that failed to persist.
//
// This actor is spawned as a child of the [EventSourcedActor]. It receives
// [persistEventsRequest] messages via Ask and replies with
// [persistEventsResponse] indicating success or failure.
type eventsWriterActor struct {
	eventsStore  persistence.EventsStore
	eventsStream eventstream.Stream
}

var _ goakt.Actor = (*eventsWriterActor)(nil)

// newEventsWriterActor creates an instance of [eventsWriterActor].
func newEventsWriterActor() *eventsWriterActor {
	return &eventsWriterActor{}
}

// PreStart loads the events store and event stream from the actor system extensions.
func (a *eventsWriterActor) PreStart(ctx *goakt.Context) error {
	a.eventsStore = ctx.Extension(extensions.EventsStoreExtensionID).(*extensions.EventsStore).Underlying()
	a.eventsStream = ctx.Extension(extensions.EventsStreamExtensionID).(*extensions.EventsStream).Underlying()
	return nil
}

// Receive handles incoming messages. Only persistEventsRequest is expected.
func (a *eventsWriterActor) Receive(ctx *goakt.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goakt.PostStart:
		// no-op
	case *persistEventsRequest:
		a.handlePersistEvents(ctx, msg)
	default:
		ctx.Unhandled()
	}
}

// PostStop performs cleanup when the actor is stopped.
func (a *eventsWriterActor) PostStop(_ *goakt.Context) error {
	return nil
}

// handlePersistEvents writes events to the store and publishes them to the stream
// only after the write succeeds. The result including any error is returned via
// Response so the parent receives the reply through its Ask call.
func (a *eventsWriterActor) handlePersistEvents(ctx *goakt.ReceiveContext, req *persistEventsRequest) {
	if err := a.eventsStore.WriteEvents(ctx.Context(), req.envelopes); err != nil {
		ctx.Response(&persistEventsResponse{Err: err})
		return
	}

	for _, envelope := range req.envelopes {
		a.eventsStream.Publish(req.topic, envelope)
	}

	ctx.Response(&persistEventsResponse{})
}
