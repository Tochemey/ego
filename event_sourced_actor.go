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
	"fmt"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/supervisor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/runner"
	"github.com/tochemey/ego/v4/persistence"
)

const (
	eventsTopic              = "topic.events.%d"
	eventsWriterChildName    = "events-writer"
	snapshotsWriterChildName = "snapshots-writer"
	eventsJanitorChildName   = "events-janitor"
	defaultPersistTimeout    = 30 * time.Second
	defaultBatchFlushWindow  = 5 * time.Millisecond
)

// persistPhase tracks the batch processing state of an [EventSourcedActor].
type persistPhase int

const (
	// phaseProcessing accepts commands and accumulates their events into
	// the batch buffer.
	phaseProcessing persistPhase = iota
	// phaseFlushing indicates that a batch write is in flight. Incoming
	// commands are stashed until the writer responds.
	phaseFlushing
	// phaseReplying sends pre-computed replies to stashed commands after
	// the batch write has been confirmed (or failed).
	phaseReplying
)

// batchFlushTick is an internal timer message sent to self to trigger
// a batch flush when the flush window expires before the batch threshold
// is reached.
type batchFlushTick struct{}

// batchEntry holds the pre-computed reply and observability context for
// a single command that has been optimistically processed and stashed
// while awaiting batch persistence.
type batchEntry struct {
	reply     *egopb.CommandReply
	startTime time.Time
	span      trace.Span
}

// EventSourcedActor persists state changes as a sequence of immutable events.
//
// Command processing generates events that are persisted through a child
// [eventsWriterActor] before the in-memory state is updated. This guarantees
// that the actor state always matches what is stored.
//
// The persistence Ask blocks the actor while the write is in flight. Incoming
// commands queue in the mailbox and are processed in order after the write
// completes, preserving command ordering and preventing concurrent state
// mutations.
//
// Snapshots and retention cleanup are handled asynchronously by dedicated child
// actors and never add latency to command processing.
type EventSourcedActor struct {
	behavior         EventSourcedBehavior
	eventsStore      persistence.EventsStore
	snapshotStore    persistence.SnapshotStore
	currentState     State
	eventsCounter    uint64
	lastCommandTime  time.Time
	eventsStream     eventstream.Stream
	persistenceID    string
	eventAdapters    []eventadapter.EventAdapter
	snapshotInterval uint64
	retentionPolicy  *RetentionPolicy
	encryptor        encryption.Encryptor
	tracer           trace.Tracer
	metrics          *metrics

	eventsWriter    *goakt.PID
	snapshotsWriter *goakt.PID
	eventsJanitor   *goakt.PID
	persistTimeout  time.Duration

	// Event batching fields. Active only when batchThreshold > 0.
	batchThreshold   int
	batchFlushWindow time.Duration
	phase            persistPhase
	batchBuffer      []*egopb.Event
	batchEntries     []batchEntry
	batchState       State
	batchCounter     uint64
	batchTime        time.Time
	batchNumEvents   int
	remainingReplies int
	shutdownOnDrain  bool
	flushTimer       *time.Timer
}

var _ goakt.Actor = (*EventSourcedActor)(nil)

// newEventSourcedActor creates an instance of EventSourcedActor.
// The constructor takes no arguments to support cluster relocation. Per-entity
// configuration is injected via the entityConfig dependency at startup.
func newEventSourcedActor() *EventSourcedActor {
	return &EventSourcedActor{}
}

// PreStart loads extensions and dependencies, validates configuration, and
// recovers the actor state from the events and snapshot stores. Child actors
// are spawned in PostStart where [goakt.ReceiveContext] is available.
func (entity *EventSourcedActor) PreStart(ctx *goakt.Context) error {
	entity.eventsStore = ctx.Extension(extensions.EventsStoreExtensionID).(*extensions.EventsStore).Underlying()
	entity.eventsStream = ctx.Extension(extensions.EventsStreamExtensionID).(*extensions.EventsStream).Underlying()
	entity.persistenceID = ctx.ActorName()
	entity.persistTimeout = defaultPersistTimeout

	entity.loadOptionalExtensions(ctx)
	entity.loadDependencies(ctx)

	if err := entity.validateAndRecover(ctx); err != nil {
		return err
	}

	if entity.metrics != nil {
		entity.metrics.entitiesActive.Add(ctx.Context(), 1)
	}

	return nil
}

// Receive is the default message handler for the actor mailbox.
//
// When event batching is enabled (batchThreshold > 0) additional internal
// message types are handled: batchFlushTick triggers a timer-based flush,
// and persistEventsResponse carries the result of an asynchronous batch write.
func (entity *EventSourcedActor) Receive(ctx *goakt.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goakt.PostStart:
		entity.spawnChildren(ctx)
	case *egopb.GetStateCommand:
		entity.getStateAndReply(ctx)
	case *batchFlushTick:
		entity.handleBatchFlushTick(ctx)
	case *persistEventsResponse:
		entity.handleBatchPersistResponse(ctx, msg)
	default:
		command, ok := msg.(Command)
		if !ok {
			ctx.Unhandled()
			return
		}
		if entity.batchEnabled() {
			entity.handleCommandBatched(ctx, command)
			return
		}
		entity.processCommandAndReply(ctx, command)
	}
}

// PostStop releases resources and resets counters when the actor shuts down.
// nolint
func (entity *EventSourcedActor) PostStop(ctx *goakt.Context) error {
	if entity.metrics != nil {
		entity.metrics.entitiesActive.Add(ctx.Context(), -1)
	}
	entity.stopFlushTimer()
	entity.resetBatch()
	entity.eventsCounter = 0
	entity.eventsWriter = nil
	entity.snapshotsWriter = nil
	entity.eventsJanitor = nil
	return nil
}

// loadOptionalExtensions reads optional extensions from the actor system.
// Missing extensions are silently skipped.
func (entity *EventSourcedActor) loadOptionalExtensions(ctx *goakt.Context) {
	if ext := ctx.Extension(extensions.SnapshotStoreExtensionID); ext != nil {
		entity.snapshotStore = ext.(*extensions.SnapshotStoreExt).Underlying()
	}

	if ext := ctx.Extension(extensions.EventAdaptersExtensionID); ext != nil {
		entity.eventAdapters = ext.(*extensions.EventAdapters).Adapters()
	}

	if ext := ctx.Extension(extensions.EncryptorExtensionID); ext != nil {
		entity.encryptor = ext.(*extensions.EncryptorExtension).Encryptor()
	}

	if ext := ctx.Extension(extensions.TelemetryExtensionID); ext != nil {
		telExt := ext.(*extensions.TelemetryExtension)
		entity.tracer = telExt.Tracer()
		entity.metrics = newMetrics(telExt.Meter())
	}
}

// loadDependencies reads the behavior and entity configuration from the
// injected dependencies.
func (entity *EventSourcedActor) loadDependencies(ctx *goakt.Context) {
	for _, dependency := range ctx.Dependencies() {
		if dependency == nil {
			continue
		}

		if behavior, ok := dependency.(EventSourcedBehavior); ok {
			entity.behavior = behavior
		}

		if config, ok := dependency.(*extensions.EntityConfig); ok {
			entity.snapshotInterval = config.SnapshotInterval
			if config.HasRetentionPolicy {
				entity.retentionPolicy = &RetentionPolicy{
					DeleteEventsOnSnapshot:    config.DeleteEventsOnSnapshot,
					DeleteSnapshotsOnSnapshot: config.DeleteSnapshotsOnSnapshot,
					EventsRetentionCount:      config.EventsRetentionCount,
				}
			}
			entity.batchThreshold = config.BatchThreshold
			entity.batchFlushWindow = config.BatchFlushWindow
			if entity.batchThreshold > 0 && entity.batchFlushWindow == 0 {
				entity.batchFlushWindow = defaultBatchFlushWindow
			}
		}
	}
}

// validateAndRecover ensures required dependencies are present, pings the
// backing stores, and replays persisted state.
func (entity *EventSourcedActor) validateAndRecover(ctx *goakt.Context) error {
	chain := runner.
		New(runner.WithFailFast()).
		AddRunner(func() error {
			if entity.behavior == nil {
				return fmt.Errorf("behavior is required")
			}
			return nil
		}).
		AddRunner(func() error { return entity.eventsStore.Ping(ctx.Context()) })

	if entity.snapshotStore != nil {
		chain = chain.AddRunner(func() error { return entity.snapshotStore.Ping(ctx.Context()) })
	}

	chain = chain.AddRunner(func() error { return entity.recover(ctx.Context()) })
	return chain.Run()
}

// childSpawnOptions returns the shared spawn options for persistence child
// actors. Children are long-lived and supervised with a restart directive so
// they recover automatically on transient failures.
func childSpawnOptions() []goakt.SpawnOption {
	return []goakt.SpawnOption{
		goakt.WithLongLived(),
		goakt.WithSupervisor(
			supervisor.NewSupervisor(
				supervisor.WithAnyErrorDirective(supervisor.RestartDirective),
			),
		),
	}
}

// spawnChildren creates the child actors responsible for event persistence,
// snapshot writes, and retention cleanup. Each child accesses its backing store
// through the actor system extensions.
func (entity *EventSourcedActor) spawnChildren(ctx *goakt.ReceiveContext) {
	opts := childSpawnOptions()

	entity.eventsWriter = ctx.Spawn(eventsWriterChildName, newEventsWriterActor(), opts...)

	if entity.snapshotStore != nil {
		entity.snapshotsWriter = ctx.Spawn(snapshotsWriterChildName, newSnapshotsWriterActor(), opts...)
	}

	if entity.retentionPolicy != nil {
		entity.eventsJanitor = ctx.Spawn(eventsJanitorChildName, newEventsJanitorActor(), opts...)
	}
}

// recover rebuilds the actor state from the snapshot and events stores.
//
// The recovery strategy is:
//  1. Load the latest snapshot (if a snapshot store is configured) to seed state.
//  2. Determine the latest persisted event sequence number.
//  3. Replay all events after the snapshot point to bring state up to date.
func (entity *EventSourcedActor) recover(ctx context.Context) error {
	state := entity.behavior.InitialState()
	replayFrom := uint64(1)

	if entity.snapshotStore != nil {
		var err error
		state, replayFrom, err = entity.recoverFromSnapshot(ctx, state)
		if err != nil {
			return err
		}
	}

	latestEvent, err := entity.eventsStore.GetLatestEvent(ctx, entity.persistenceID)
	if err != nil {
		return fmt.Errorf("failed to get latest event: %w", err)
	}

	if latestEvent == nil {
		entity.currentState = state
		return nil
	}

	latestSeqNr := latestEvent.GetSequenceNumber()

	if replayFrom <= latestSeqNr {
		state, err = entity.replayEvents(ctx, state, replayFrom, latestSeqNr)
		if err != nil {
			return err
		}
		entity.eventsCounter = latestSeqNr
	}

	entity.currentState = state
	return nil
}

// recoverFromSnapshot loads the latest snapshot and returns the restored state
// together with the sequence number to replay from. When no snapshot exists the
// initial state and a replayFrom of 1 are returned unchanged.
func (entity *EventSourcedActor) recoverFromSnapshot(ctx context.Context, initial State) (State, uint64, error) {
	snapshot, err := entity.snapshotStore.GetLatestSnapshot(ctx, entity.persistenceID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to load snapshot: %w", err)
	}

	if snapshot == nil || snapshot.GetState() == nil {
		return initial, 1, nil
	}

	snapshotState, err := entity.decryptPayload(ctx, snapshot.GetState(), snapshot.GetIsEncrypted(), snapshot.GetEncryptionKeyId())
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decrypt snapshot: %w", err)
	}

	if err := snapshotState.UnmarshalTo(initial); err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal snapshot state: %w", err)
	}

	seqNr := snapshot.GetSequenceNumber()
	entity.eventsCounter = seqNr
	return initial, seqNr + 1, nil
}

// replayEvents applies persisted events to the given state in sequence order
// and returns the resulting state.
func (entity *EventSourcedActor) replayEvents(ctx context.Context, state State, from, to uint64) (State, error) {
	events, err := entity.eventsStore.ReplayEvents(ctx, entity.persistenceID, from, to, to-from+1)
	if err != nil {
		return nil, fmt.Errorf("failed to replay events: %w", err)
	}

	for _, envelope := range events {
		state, err = entity.applyPersistedEvent(ctx, envelope, state)
		if err != nil {
			return nil, err
		}
	}

	return state, nil
}

// applyPersistedEvent decrypts, adapts, and applies a single persisted event
// envelope to the given state.
func (entity *EventSourcedActor) applyPersistedEvent(ctx context.Context, envelope *egopb.Event, state State) (State, error) {
	seqNr := envelope.GetSequenceNumber()

	evt, err := entity.decryptPayload(ctx, envelope.GetEvent(), envelope.GetIsEncrypted(), envelope.GetEncryptionKeyId())
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt event at sequence %d: %w", seqNr, err)
	}

	if len(entity.eventAdapters) > 0 {
		evt, err = eventadapter.Chain(entity.eventAdapters, evt, seqNr)
		if err != nil {
			return nil, fmt.Errorf("failed to adapt event at sequence %d: %w", seqNr, err)
		}
	}

	eventMsg, err := evt.UnmarshalNew()
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal event at sequence %d: %w", seqNr, err)
	}

	state, err = entity.behavior.HandleEvent(ctx, eventMsg.(Event), state)
	if err != nil {
		return nil, fmt.Errorf("failed to handle event at sequence %d: %w", seqNr, err)
	}

	return state, nil
}

// decryptPayload decrypts an [anypb.Any] payload when encryption is enabled.
// Unencrypted payloads are returned as-is.
func (entity *EventSourcedActor) decryptPayload(ctx context.Context, payload *anypb.Any, encrypted bool, keyID string) (*anypb.Any, error) {
	if !encrypted || entity.encryptor == nil {
		return payload, nil
	}

	plaintext, err := entity.encryptor.Decrypt(ctx, entity.persistenceID, payload.GetValue(), keyID)
	if err != nil {
		return nil, err
	}

	decrypted := &anypb.Any{TypeUrl: payload.GetTypeUrl()}
	if err := proto.Unmarshal(plaintext, decrypted); err != nil {
		return nil, fmt.Errorf("failed to unmarshal decrypted payload: %w", err)
	}

	return decrypted, nil
}

// sendErrorReply sends a [egopb.CommandReply] containing the given error.
func (entity *EventSourcedActor) sendErrorReply(ctx *goakt.ReceiveContext, err error) {
	ctx.Response(&egopb.CommandReply{
		Reply: &egopb.CommandReply_ErrorReply{
			ErrorReply: &egopb.ErrorReply{
				Message: err.Error(),
			},
		},
	})
}

// sendStateReply sends a [egopb.CommandReply] containing the current state.
func (entity *EventSourcedActor) sendStateReply(ctx *goakt.ReceiveContext) {
	state, _ := anypb.New(entity.currentState)
	ctx.Response(&egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.persistenceID,
				State:          state,
				SequenceNumber: entity.eventsCounter,
				Timestamp:      entity.lastCommandTime.Unix(),
			},
		},
	})
}

// getStateAndReply returns the last committed state of the entity without
// processing any command. When event batching is enabled, this returns the
// state as of the last confirmed batch write, not any optimistic pending state.
func (entity *EventSourcedActor) getStateAndReply(ctx *goakt.ReceiveContext) {
	entity.sendStateReply(ctx)
}

// processCommandAndReply handles an incoming command by generating events,
// persisting them through the [eventsWriterActor], and applying state changes
// only after persistence is confirmed.
//
// On persistence failure the actor replies with an error and shuts itself down
// so the supervisor can restart it with clean state recovered from the store.
func (entity *EventSourcedActor) processCommandAndReply(ctx *goakt.ReceiveContext, command Command) {
	goCtx := ctx.Context()
	startTime := time.Now()

	if entity.tracer != nil {
		var span trace.Span
		goCtx, span = entity.tracer.Start(goCtx, "ego.command",
			trace.WithAttributes(
				attribute.String("ego.persistence_id", entity.persistenceID),
				attribute.String("ego.command_type", string(command.ProtoReflect().Descriptor().FullName())),
			))
		defer span.End()
	}

	if entity.metrics != nil {
		entity.metrics.commandsTotal.Add(goCtx, 1)
		defer func() {
			duration := float64(time.Since(startTime).Milliseconds())
			entity.metrics.commandsDuration.Record(goCtx, duration)
		}()
	}

	events, err := entity.behavior.HandleCommand(goCtx, command, entity.currentState)
	if err != nil {
		entity.sendErrorReply(ctx, err)
		return
	}

	if len(events) == 0 {
		entity.sendStateReply(ctx)
		return
	}

	envelopes, pendingState, pendingCounter, commandTime, err := entity.buildEnvelopes(goCtx, ctx, events, entity.currentState, entity.eventsCounter)
	if err != nil {
		entity.sendErrorReply(ctx, err)
		return
	}

	topic := fmt.Sprintf(eventsTopic, ctx.ActorSystem().Partition(entity.persistenceID))
	if err := entity.persistEvents(ctx, envelopes, topic); err != nil {
		entity.sendErrorReply(ctx, err)
		ctx.Shutdown()
		return
	}

	entity.applyConfirmedState(goCtx, pendingState, pendingCounter, commandTime, len(envelopes))
	entity.triggerSnapshotAndRetention(ctx)
	entity.sendStateReply(ctx)
}

// buildEnvelopes computes the pending state from the given events and creates
// the protobuf envelopes ready for persistence. The actor state is not modified.
// startState and startCounter specify the base state and sequence number to
// apply events against, allowing callers to chain calls across batched commands.
// Returns the envelopes, pending state, pending counter, and command timestamp.
func (entity *EventSourcedActor) buildEnvelopes(goCtx context.Context, ctx *goakt.ReceiveContext, events []Event, startState State, startCounter uint64) ([]*egopb.Event, State, uint64, time.Time, error) {
	shardNumber := ctx.ActorSystem().Partition(entity.persistenceID)
	pendingState := startState
	pendingCounter := startCounter
	commandTime := timestamppb.Now().AsTime()

	envelopes := make([]*egopb.Event, 0, len(events))
	for _, event := range events {
		resultingState, err := entity.behavior.HandleEvent(goCtx, event, pendingState)
		if err != nil {
			return nil, nil, 0, time.Time{}, err
		}

		pendingCounter++
		pendingState = resultingState

		envelope, err := entity.marshalEvent(goCtx, event, pendingCounter, commandTime, shardNumber)
		if err != nil {
			return nil, nil, 0, time.Time{}, err
		}

		envelopes = append(envelopes, envelope)
	}

	return envelopes, pendingState, pendingCounter, commandTime, nil
}

// marshalEvent serializes a domain event into a protobuf envelope, applying
// encryption when an encryptor is configured.
func (entity *EventSourcedActor) marshalEvent(ctx context.Context, event Event, seqNr uint64, ts time.Time, shard uint64) (*egopb.Event, error) {
	eventAny, _ := anypb.New(event)

	var encKeyID string
	var isEncrypted bool

	if entity.encryptor != nil {
		eventBytes, err := proto.Marshal(eventAny)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal event for encryption: %w", err)
		}

		ciphertext, keyID, err := entity.encryptor.Encrypt(ctx, entity.persistenceID, eventBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt event: %w", err)
		}

		eventAny = &anypb.Any{
			TypeUrl: eventAny.GetTypeUrl(),
			Value:   ciphertext,
		}
		encKeyID = keyID
		isEncrypted = true
	}

	return &egopb.Event{
		PersistenceId:   entity.persistenceID,
		SequenceNumber:  seqNr,
		IsDeleted:       false,
		Event:           eventAny,
		Timestamp:       ts.Unix(),
		Shard:           shard,
		EncryptionKeyId: encKeyID,
		IsEncrypted:     isEncrypted,
	}, nil
}

// persistEvents sends the event envelopes to the [eventsWriterActor] via a
// synchronous Ask and returns an error when persistence fails.
func (entity *EventSourcedActor) persistEvents(ctx *goakt.ReceiveContext, envelopes []*egopb.Event, topic string) error {
	reply := ctx.Ask(entity.eventsWriter, &persistEventsRequest{
		envelopes: envelopes,
		topic:     topic,
	}, entity.persistTimeout)

	if reply == nil {
		return fmt.Errorf("event writer returned no response")
	}

	persistReply, ok := reply.(*persistEventsResponse)
	if !ok {
		return fmt.Errorf("unexpected response type %T from event writer", reply)
	}

	return persistReply.Err
}

// applyConfirmedState updates the actor state after the events store has
// confirmed the write and records persistence metrics.
func (entity *EventSourcedActor) applyConfirmedState(goCtx context.Context, state State, counter uint64, ts time.Time, numEvents int) {
	entity.eventsCounter = counter
	entity.currentState = state
	entity.lastCommandTime = ts

	if entity.metrics != nil {
		entity.metrics.eventsPersisted.Add(goCtx, int64(numEvents))
	}
}

// triggerSnapshotAndRetention fires an asynchronous snapshot request to the
// dedicated child actor when the snapshot interval is reached. If a retention
// policy is active, the retention request is bundled into the snapshot request
// so the snapshot writer forwards it to the janitor only after the snapshot is
// confirmed persisted. This prevents the race where retention could delete old
// data before the new snapshot is safely written.
func (entity *EventSourcedActor) triggerSnapshotAndRetention(ctx *goakt.ReceiveContext) {
	if entity.snapshotsWriter == nil || entity.snapshotInterval == 0 || entity.eventsCounter%entity.snapshotInterval != 0 {
		return
	}
	entity.snapshotAndRetain(ctx)
}

// snapshotAndRetain creates a snapshot of the current confirmed state and
// sends it to the snapshot writer child. If retention is enabled, the
// retention request is bundled so cleanup occurs only after the snapshot
// is confirmed persisted.
func (entity *EventSourcedActor) snapshotAndRetain(ctx *goakt.ReceiveContext) {
	stateAny, _ := anypb.New(entity.currentState)
	req := &persistSnapshotRequest{
		snapshot: entity.newSnapshotEnvelope(stateAny),
	}

	if entity.eventsJanitor != nil && entity.retentionPolicy != nil {
		req.retentionReq = &applyRetentionRequest{
			persistenceID:             entity.persistenceID,
			eventsCounter:             entity.eventsCounter,
			snapshotInterval:          entity.snapshotInterval,
			deleteEventsOnSnapshot:    entity.retentionPolicy.DeleteEventsOnSnapshot,
			deleteSnapshotsOnSnapshot: entity.retentionPolicy.DeleteSnapshotsOnSnapshot,
			eventsRetentionCount:      entity.retentionPolicy.EventsRetentionCount,
		}
		req.janitor = entity.eventsJanitor
	}

	ctx.Tell(entity.snapshotsWriter, req)
}

// newSnapshotEnvelope creates a [egopb.Snapshot] with unencrypted state from
// the current entity. Encryption is handled by the [snapshotsWriterActor].
func (entity *EventSourcedActor) newSnapshotEnvelope(state *anypb.Any) *egopb.Snapshot {
	return &egopb.Snapshot{
		PersistenceId:  entity.persistenceID,
		SequenceNumber: entity.eventsCounter,
		State:          state,
		Timestamp:      entity.lastCommandTime.Unix(),
	}
}

// batchEnabled reports whether event batching is active for this entity.
func (entity *EventSourcedActor) batchEnabled() bool {
	return entity.batchThreshold > 0
}

// latestState returns the most recent state, which may be an unconfirmed
// pending state from the current batch or the last committed state.
func (entity *EventSourcedActor) latestState() State {
	if len(entity.batchEntries) > 0 {
		return entity.batchState
	}
	return entity.currentState
}

// latestCounter returns the most recent sequence number, which may include
// unconfirmed events from the current batch.
func (entity *EventSourcedActor) latestCounter() uint64 {
	if len(entity.batchEntries) > 0 {
		return entity.batchCounter
	}
	return entity.eventsCounter
}

// handleCommandBatched dispatches an incoming command according to the
// current batch processing phase.
func (entity *EventSourcedActor) handleCommandBatched(ctx *goakt.ReceiveContext, command Command) {
	switch entity.phase {
	case phaseProcessing:
		entity.processAndBatch(ctx, command)
	case phaseFlushing:
		ctx.Stash()
	case phaseReplying:
		entity.replyFromBatch(ctx)
	}
}

// processAndBatch processes a command optimistically against the latest
// (possibly unconfirmed) state, appends the resulting event envelopes to
// the batch buffer, stashes the command so its response channel is
// preserved, and triggers a flush when the batch threshold is reached.
func (entity *EventSourcedActor) processAndBatch(ctx *goakt.ReceiveContext, command Command) {
	goCtx := ctx.Context()
	startTime := time.Now()

	var span trace.Span
	if entity.tracer != nil {
		goCtx, span = entity.tracer.Start(goCtx, "ego.command",
			trace.WithAttributes(
				attribute.String("ego.persistence_id", entity.persistenceID),
				attribute.String("ego.command_type", string(command.ProtoReflect().Descriptor().FullName())),
			))
	}

	if entity.metrics != nil {
		entity.metrics.commandsTotal.Add(goCtx, 1)
	}

	state := entity.latestState()
	counter := entity.latestCounter()

	events, err := entity.behavior.HandleCommand(goCtx, command, state)
	if err != nil {
		if span != nil {
			span.End()
		}
		entity.sendErrorReply(ctx, err)
		return
	}

	if len(events) == 0 {
		if span != nil {
			span.End()
		}
		stateAny, _ := anypb.New(state)
		ctx.Response(&egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  entity.persistenceID,
					State:          stateAny,
					SequenceNumber: counter,
					Timestamp:      entity.lastCommandTime.Unix(),
				},
			},
		})
		return
	}

	envelopes, pendingState, pendingCounter, commandTime, err := entity.buildEnvelopes(goCtx, ctx, events, state, counter)
	if err != nil {
		if span != nil {
			span.End()
		}
		entity.sendErrorReply(ctx, err)
		return
	}

	entity.batchBuffer = append(entity.batchBuffer, envelopes...)
	entity.batchState = pendingState
	entity.batchCounter = pendingCounter
	entity.batchTime = commandTime
	entity.batchNumEvents += len(envelopes)

	stateAny, _ := anypb.New(pendingState)
	entity.batchEntries = append(entity.batchEntries, batchEntry{
		reply: &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  entity.persistenceID,
					State:          stateAny,
					SequenceNumber: pendingCounter,
					Timestamp:      commandTime.Unix(),
				},
			},
		},
		startTime: startTime,
		span:      span,
	})

	ctx.Stash()

	if len(entity.batchEntries) >= entity.batchThreshold {
		entity.flushBatch(ctx)
		return
	}

	entity.startFlushTimer(ctx)
}

// flushBatch sends the accumulated event envelopes to the events writer
// asynchronously via PipeTo and transitions to phaseFlushing. The writer
// is called through goakt.Ask inside a goroutine so the actor remains
// responsive while the write is in flight.
func (entity *EventSourcedActor) flushBatch(ctx *goakt.ReceiveContext) {
	entity.stopFlushTimer()

	topic := fmt.Sprintf(eventsTopic, ctx.ActorSystem().Partition(entity.persistenceID))
	envelopes := entity.batchBuffer
	writer := entity.eventsWriter
	timeout := entity.persistTimeout

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		reply, err := goakt.Ask(context.Background(), writer, &persistEventsRequest{
			envelopes: envelopes,
			topic:     topic,
		}, timeout)

		if err != nil {
			return &persistEventsResponse{Err: err}, nil
		}

		if reply == nil {
			return &persistEventsResponse{Err: fmt.Errorf("event writer returned no response")}, nil
		}

		resp, ok := reply.(*persistEventsResponse)
		if !ok {
			return &persistEventsResponse{Err: fmt.Errorf("unexpected response type %T from event writer", reply)}, nil
		}
		return resp, nil
	})

	entity.phase = phaseFlushing
}

// handleBatchFlushTick is invoked when the flush window timer expires.
// If the actor is still in phaseProcessing with a non-empty batch, the
// batch is flushed immediately.
func (entity *EventSourcedActor) handleBatchFlushTick(ctx *goakt.ReceiveContext) {
	entity.flushTimer = nil
	if entity.phase != phaseProcessing || len(entity.batchEntries) == 0 {
		return
	}
	entity.flushBatch(ctx)
}

// handleBatchPersistResponse processes the result delivered by PipeTo after
// the events writer completes a batch write.
//
// On success: the confirmed state is committed, snapshot and retention are
// triggered when applicable, and all stashed commands are unstashed so each
// caller receives its pre-computed reply.
//
// On failure: all pre-computed replies are replaced with error replies, the
// stashed commands are unstashed so callers are notified, and the actor
// shuts down after all replies have been sent so the supervisor can restart
// it with clean state.
func (entity *EventSourcedActor) handleBatchPersistResponse(ctx *goakt.ReceiveContext, resp *persistEventsResponse) {
	if entity.phase != phaseFlushing {
		return
	}

	if resp.Err != nil {
		errReply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_ErrorReply{
				ErrorReply: &egopb.ErrorReply{
					Message: resp.Err.Error(),
				},
			},
		}
		for i := range entity.batchEntries {
			if entity.batchEntries[i].span != nil {
				entity.batchEntries[i].span.End()
				entity.batchEntries[i].span = nil
			}
			entity.batchEntries[i].reply = errReply
		}
		entity.remainingReplies = len(entity.batchEntries)
		entity.shutdownOnDrain = true
		entity.phase = phaseReplying
		ctx.UnstashAll()
		return
	}

	previousCounter := entity.eventsCounter
	entity.applyConfirmedState(ctx.Context(), entity.batchState, entity.batchCounter, entity.batchTime, entity.batchNumEvents)

	if entity.crossedSnapshotBoundary(previousCounter) {
		entity.snapshotAndRetain(ctx)
	}

	entity.remainingReplies = len(entity.batchEntries)
	entity.phase = phaseReplying
	ctx.UnstashAll()
}

// replyFromBatch sends the next pre-computed reply to the current (unstashed)
// caller. When all stashed commands have been answered, the actor resets its
// batch state and returns to phaseProcessing, or shuts down if the preceding
// batch write failed.
//
// If more messages were unstashed than there are pending replies (e.g. commands
// that arrived during phaseFlushing), the surplus messages are processed as new
// commands in a fresh batch cycle.
func (entity *EventSourcedActor) replyFromBatch(ctx *goakt.ReceiveContext) {
	if entity.remainingReplies <= 0 {
		entity.phase = phaseProcessing
		command, ok := ctx.Message().(Command)
		if !ok {
			ctx.Unhandled()
			return
		}
		entity.processAndBatch(ctx, command)
		return
	}

	idx := len(entity.batchEntries) - entity.remainingReplies
	entry := &entity.batchEntries[idx]

	if entry.span != nil {
		entry.span.End()
		entry.span = nil
	}

	if entity.metrics != nil {
		duration := float64(time.Since(entry.startTime).Milliseconds())
		entity.metrics.commandsDuration.Record(ctx.Context(), duration)
	}

	ctx.Response(entry.reply)
	entity.remainingReplies--

	if entity.remainingReplies == 0 {
		shouldShutdown := entity.shutdownOnDrain
		entity.resetBatch()
		entity.phase = phaseProcessing
		if shouldShutdown {
			ctx.Shutdown()
		}
	}
}

// crossedSnapshotBoundary reports whether the range
// (previousCounter, entity.eventsCounter] contains at least one multiple
// of snapshotInterval.
func (entity *EventSourcedActor) crossedSnapshotBoundary(previousCounter uint64) bool {
	if entity.snapshotsWriter == nil || entity.snapshotInterval == 0 {
		return false
	}
	return entity.eventsCounter/entity.snapshotInterval > previousCounter/entity.snapshotInterval
}

// startFlushTimer arms the batch flush timer if it is not already running.
// When the timer fires, a batchFlushTick is delivered to the actor mailbox.
func (entity *EventSourcedActor) startFlushTimer(ctx *goakt.ReceiveContext) {
	if entity.flushTimer != nil {
		return
	}
	self := ctx.Self()
	entity.flushTimer = time.AfterFunc(entity.batchFlushWindow, func() {
		_ = goakt.Tell(context.Background(), self, new(batchFlushTick))
	})
}

// stopFlushTimer cancels a running flush timer, if any.
func (entity *EventSourcedActor) stopFlushTimer() {
	if entity.flushTimer != nil {
		entity.flushTimer.Stop()
		entity.flushTimer = nil
	}
}

// resetBatch clears all batch accumulation state, preparing the actor for
// a new batch cycle.
func (entity *EventSourcedActor) resetBatch() {
	entity.batchBuffer = nil
	entity.batchEntries = nil
	entity.batchState = nil
	entity.batchCounter = 0
	entity.batchTime = time.Time{}
	entity.batchNumEvents = 0
	entity.remainingReplies = 0
	entity.shutdownOnDrain = false
}
