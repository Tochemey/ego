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

package extensions

import (
	"time"

	"github.com/tochemey/goakt/v3/extension"

	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/offsetstore"
	"github.com/tochemey/ego/v3/persistence"
	"github.com/tochemey/ego/v3/projection"
)

const (
	// EventsStoreExtensionID is the identifier for the pluggable event store extension.
	// It is used to persist domain events for event-sourced entities.
	EventsStoreExtensionID = "EgoEventsStoreExtension"

	// DurableStateStoreExtensionID is the identifier for the durable state store extension.
	// It is used to persist the current state of entities that use state-based persistence.
	DurableStateStoreExtensionID = "EgoStatesStoreExtension"

	// EventsStreamExtensionID is the identifier for the event stream extension.
	// It enables streaming of events to external subscribers or processors.
	EventsStreamExtensionID = "EgoEventStreamExtension"

	// OffsetStoreExtensionID is the identifier for the offset store extension.
	OffsetStoreExtensionID = "EgoOffsetStoreExtension"

	// ProjectionExtensionID is the identifier for the pluggable projection handler extension.
	ProjectionExtensionID = "EgoProjectionExtension"
)

type EventsStore struct {
	underlying persistence.EventsStore
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*EventsStore)(nil)

// NewEventsStore creates a new events store
func NewEventsStore(store persistence.EventsStore) *EventsStore {
	return &EventsStore{
		underlying: store,
	}
}

// ID returns the id of the events store
func (x *EventsStore) ID() string {
	return EventsStoreExtensionID
}

// Underlying returns the handler events store
func (x *EventsStore) Underlying() persistence.EventsStore {
	return x.underlying
}

type DurableStateStore struct {
	underlying persistence.StateStore
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*DurableStateStore)(nil)

// NewDurableStateStore creates a new durable state store
func NewDurableStateStore(store persistence.StateStore) *DurableStateStore {
	return &DurableStateStore{
		underlying: store,
	}
}

// Underlying returns the handler state store
func (x *DurableStateStore) Underlying() persistence.StateStore {
	return x.underlying
}

// ID returns the id of the durable state store
func (x *DurableStateStore) ID() string {
	return DurableStateStoreExtensionID
}

type EventsStream struct {
	underlying eventstream.Stream
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*EventsStream)(nil)

// NewEventsStream creates a new events stream
func NewEventsStream(stream eventstream.Stream) *EventsStream {
	return &EventsStream{
		underlying: stream,
	}
}

// Underlying returns the handler events stream
func (x *EventsStream) Underlying() eventstream.Stream {
	return x.underlying
}

// ID returns the id of the events stream
func (x *EventsStream) ID() string {
	return EventsStreamExtensionID
}

type OffsetStore struct {
	underlying offsetstore.OffsetStore
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*OffsetStore)(nil)

// NewOffsetStore creates an instance of OffsetStore extension
func NewOffsetStore(store offsetstore.OffsetStore) *OffsetStore {
	return &OffsetStore{
		underlying: store,
	}
}

// ID returns the identifier for the OffsetStore instance.
func (x *OffsetStore) ID() string {
	return OffsetStoreExtensionID
}

// Underlying returns the handler offset store implementation used internally.
func (x *OffsetStore) Underlying() offsetstore.OffsetStore {
	return x.underlying
}

type ProjectionExtension struct {
	handler      projection.Handler
	bufferSize   int
	startOffset  time.Time
	resetOffset  time.Time
	recovery     *projection.Recovery
	pullInterval time.Duration
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*ProjectionExtension)(nil)

// NewProjectionExtension creates a new projection handler extension
func NewProjectionExtension(handler projection.Handler,
	bufferSize int,
	startOffset,
	resetOffset time.Time,
	pullInterval time.Duration,
	recovery *projection.Recovery) *ProjectionExtension {
	return &ProjectionExtension{
		handler:      handler,
		bufferSize:   bufferSize,
		startOffset:  startOffset,
		resetOffset:  resetOffset,
		recovery:     recovery,
		pullInterval: pullInterval,
	}
}

// ID returns the identifier of the extension
func (x *ProjectionExtension) ID() string {
	return ProjectionExtensionID
}

// Handler returns the projection handler handler
func (x *ProjectionExtension) Handler() projection.Handler {
	return x.handler
}

func (x *ProjectionExtension) BufferSize() int {
	return x.bufferSize
}

func (x *ProjectionExtension) StartOffset() time.Time {
	return x.startOffset
}

func (x *ProjectionExtension) ResetOffset() time.Time {
	return x.resetOffset
}

func (x *ProjectionExtension) Recovery() *projection.Recovery {
	return x.recovery
}

func (x *ProjectionExtension) PullInterval() time.Duration {
	return x.pullInterval
}
