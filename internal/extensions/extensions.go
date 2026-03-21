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
	"encoding/json"
	"time"

	"github.com/tochemey/goakt/v4/extension"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/offsetstore"
	"github.com/tochemey/ego/v4/persistence"
	"github.com/tochemey/ego/v4/projection"
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

	// EventAdaptersExtensionID is the identifier for the event adapters extension.
	// It is used to transform persisted events from older schema versions during replay and projection consumption.
	EventAdaptersExtensionID = "EgoEventAdaptersExtension"

	// SnapshotStoreExtensionID is the identifier for the snapshot store extension.
	// It is used to persist entity state snapshots separately from events.
	SnapshotStoreExtensionID = "EgoSnapshotStoreExtension"

	// TelemetryExtensionID is the identifier for the telemetry extension.
	// It carries OpenTelemetry tracer and metrics instruments to actors.
	TelemetryExtensionID = "EgoTelemetryExtension"

	// EncryptorExtensionID is the identifier for the encryptor extension.
	// It carries the event/snapshot encryptor to actors.
	EncryptorExtensionID = "EgoEncryptorExtension"

	// EntityConfigID is the identifier for the entity config dependency.
	EntityConfigID = "EgoEntityConfig"

	// SagaConfigID is the identifier for the saga config dependency.
	SagaConfigID = "EgoSagaConfig"
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
	handler           projection.Handler
	bufferSize        int
	startOffset       time.Time
	resetOffset       time.Time
	recovery          *projection.Recovery
	pullInterval      time.Duration
	deadLetterHandler projection.DeadLetterHandler
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*ProjectionExtension)(nil)

// NewProjectionExtension creates a new projection handler extension
func NewProjectionExtension(handler projection.Handler,
	bufferSize int,
	startOffset,
	resetOffset time.Time,
	pullInterval time.Duration,
	recovery *projection.Recovery,
	deadLetterHandler projection.DeadLetterHandler) *ProjectionExtension {
	return &ProjectionExtension{
		handler:           handler,
		bufferSize:        bufferSize,
		startOffset:       startOffset,
		resetOffset:       resetOffset,
		recovery:          recovery,
		pullInterval:      pullInterval,
		deadLetterHandler: deadLetterHandler,
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

// DeadLetterHandler returns the dead letter handler
func (x *ProjectionExtension) DeadLetterHandler() projection.DeadLetterHandler {
	return x.deadLetterHandler
}

// SnapshotStoreExt wraps a persistence.SnapshotStore for use as an extension.
type SnapshotStoreExt struct {
	underlying persistence.SnapshotStore
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*SnapshotStoreExt)(nil)

// NewSnapshotStore creates a new snapshot store extension
func NewSnapshotStore(store persistence.SnapshotStore) *SnapshotStoreExt {
	return &SnapshotStoreExt{
		underlying: store,
	}
}

// ID returns the id of the snapshot store extension
func (x *SnapshotStoreExt) ID() string {
	return SnapshotStoreExtensionID
}

// Underlying returns the snapshot store implementation
func (x *SnapshotStoreExt) Underlying() persistence.SnapshotStore {
	return x.underlying
}

// EventAdapters wraps a slice of eventadapter.EventAdapter for use as an extension.
type EventAdapters struct {
	adapters []eventadapter.EventAdapter
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*EventAdapters)(nil)

// NewEventAdapters creates a new event adapters extension
func NewEventAdapters(adapters []eventadapter.EventAdapter) *EventAdapters {
	return &EventAdapters{
		adapters: adapters,
	}
}

// ID returns the identifier for the EventAdapters extension
func (x *EventAdapters) ID() string {
	return EventAdaptersExtensionID
}

// Adapters returns the underlying event adapters
func (x *EventAdapters) Adapters() []eventadapter.EventAdapter {
	return x.adapters
}

// TelemetryExtension carries OpenTelemetry tracer and metrics to actors.
type TelemetryExtension struct {
	tracer trace.Tracer
	meter  metric.Meter
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*TelemetryExtension)(nil)

// NewTelemetryExtension creates a new telemetry extension
func NewTelemetryExtension(tracer trace.Tracer, meter metric.Meter) *TelemetryExtension {
	return &TelemetryExtension{
		tracer: tracer,
		meter:  meter,
	}
}

// ID returns the identifier for the TelemetryExtension
func (x *TelemetryExtension) ID() string {
	return TelemetryExtensionID
}

// Tracer returns the OpenTelemetry tracer
func (x *TelemetryExtension) Tracer() trace.Tracer {
	return x.tracer
}

// Meter returns the OpenTelemetry meter
func (x *TelemetryExtension) Meter() metric.Meter {
	return x.meter
}

// EncryptorExtension wraps an encryption.Encryptor for use as an extension.
type EncryptorExtension struct {
	encryptor encryption.Encryptor
}

// enforce compliance with the extension.Extension interface
var _ extension.Extension = (*EncryptorExtension)(nil)

// NewEncryptor creates a new encryptor extension
func NewEncryptor(encryptor encryption.Encryptor) *EncryptorExtension {
	return &EncryptorExtension{encryptor: encryptor}
}

// ID returns the identifier for the EncryptorExtension
func (x *EncryptorExtension) ID() string {
	return EncryptorExtensionID
}

// Encryptor returns the underlying encryptor
func (x *EncryptorExtension) Encryptor() encryption.Encryptor {
	return x.encryptor
}

// EntityConfig is a dependency that carries per-entity spawn configuration.
// This allows the EventSourcedActor constructor to take no arguments,
// which is required for correct behavior during cluster relocation.
type EntityConfig struct {
	SnapshotInterval          uint64 `json:"snapshot_interval"`
	DeleteEventsOnSnapshot    bool   `json:"delete_events_on_snapshot"`
	DeleteSnapshotsOnSnapshot bool   `json:"delete_snapshots_on_snapshot"`
	EventsRetentionCount      uint64 `json:"events_retention_count"`
	HasRetentionPolicy        bool   `json:"has_retention_policy"`
}

// enforce compliance with the extension.Dependency interface
var _ extension.Dependency = (*EntityConfig)(nil)

// NewEntityConfig creates a new entity config dependency
func NewEntityConfig(snapshotInterval uint64) *EntityConfig {
	return &EntityConfig{
		SnapshotInterval: snapshotInterval,
	}
}

// ID returns the identifier for the EntityConfig
func (x *EntityConfig) ID() string {
	return EntityConfigID
}

// MarshalBinary serializes the entity config
func (x *EntityConfig) MarshalBinary() ([]byte, error) {
	return json.Marshal(x)
}

// UnmarshalBinary deserializes the entity config
func (x *EntityConfig) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, x)
}

// SagaConfig is a dependency that carries per-saga spawn configuration.
type SagaConfig struct {
	Timeout time.Duration `json:"timeout"`
}

// enforce compliance with the extension.Dependency interface
var _ extension.Dependency = (*SagaConfig)(nil)

// NewSagaConfig creates a new saga config dependency
func NewSagaConfig(timeout time.Duration) *SagaConfig {
	return &SagaConfig{Timeout: timeout}
}

// ID returns the identifier for the SagaConfig
func (x *SagaConfig) ID() string {
	return SagaConfigID
}

// MarshalBinary serializes the saga config
func (x *SagaConfig) MarshalBinary() ([]byte, error) {
	return json.Marshal(x)
}

// UnmarshalBinary deserializes the saga config
func (x *SagaConfig) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, x)
}
