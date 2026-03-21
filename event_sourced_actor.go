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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
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

var (
	eventsTopic = "topic.events.%d"
)

// EventSourcedActor is an event sourced based actor
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
}

// implements the goakt.Actor interface
var _ goakt.Actor = (*EventSourcedActor)(nil)

// newEventSourcedActor creates an instance of EventSourcedActor.
// No arguments are passed in the constructor to support cluster relocation.
// Per-entity config is passed via the entityConfig dependency.
func newEventSourcedActor() *EventSourcedActor {
	return &EventSourcedActor{}
}

// PreStart pre-starts the actor
func (entity *EventSourcedActor) PreStart(ctx *goakt.Context) error {
	entity.eventsStore = ctx.Extension(extensions.EventsStoreExtensionID).(*extensions.EventsStore).Underlying()
	entity.eventsStream = ctx.Extension(extensions.EventsStreamExtensionID).(*extensions.EventsStream).Underlying()
	entity.persistenceID = ctx.ActorName()

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

	for _, dependency := range ctx.Dependencies() {
		if dependency == nil {
			continue
		}
		if behavior, ok := dependency.(EventSourcedBehavior); ok {
			entity.behavior = behavior
		}
		if cfg, ok := dependency.(*extensions.EntityConfig); ok {
			entity.snapshotInterval = cfg.SnapshotInterval
			if cfg.HasRetentionPolicy {
				entity.retentionPolicy = &RetentionPolicy{
					DeleteEventsOnSnapshot:    cfg.DeleteEventsOnSnapshot,
					DeleteSnapshotsOnSnapshot: cfg.DeleteSnapshotsOnSnapshot,
					EventsRetentionCount:      cfg.EventsRetentionCount,
				}
			}
		}
	}

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

	if err := chain.Run(); err != nil {
		return err
	}

	if entity.metrics != nil {
		entity.metrics.entitiesActive.Add(ctx.Context(), 1)
	}

	return nil
}

// Receive processes any message dropped into the actor mailbox.
func (entity *EventSourcedActor) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		// pass
	case *egopb.GetStateCommand:
		entity.getStateAndReply(ctx)
	default:
		command := message.(Command)
		entity.processCommandAndReply(ctx, command)
	}
}

// PostStop prepares the actor to gracefully shutdown
// nolint
func (entity *EventSourcedActor) PostStop(ctx *goakt.Context) error {
	if entity.metrics != nil {
		entity.metrics.entitiesActive.Add(ctx.Context(), -1)
	}
	entity.eventsCounter = 0
	return nil
}

// recover rebuilds the actor state from the snapshot store (if available) and events store.
// Recovery strategy:
//  1. If a snapshot store is configured, load the latest snapshot to seed state.
//  2. Determine the latest event sequence number from the events store.
//  3. Replay all events after the snapshot (or from the beginning if no snapshot).
func (entity *EventSourcedActor) recover(ctx context.Context) error {
	state := entity.behavior.InitialState()
	replayFrom := uint64(1)

	// Step 1: try to load from snapshot store
	if entity.snapshotStore != nil {
		snapshot, err := entity.snapshotStore.GetLatestSnapshot(ctx, entity.persistenceID)
		if err != nil {
			return fmt.Errorf("failed to load snapshot: %w", err)
		}

		if snapshot != nil && snapshot.GetState() != nil {
			snapshotState := snapshot.GetState()
			// Decrypt the snapshot if it was encrypted
			if snapshot.GetIsEncrypted() && entity.encryptor != nil {
				plaintext, err := entity.encryptor.Decrypt(ctx, entity.persistenceID, snapshotState.GetValue(), snapshot.GetEncryptionKeyId())
				if err != nil {
					return fmt.Errorf("failed to decrypt snapshot: %w", err)
				}

				decrypted := &anypb.Any{TypeUrl: snapshotState.GetTypeUrl()}
				if err := proto.Unmarshal(plaintext, decrypted); err != nil {
					return fmt.Errorf("failed to unmarshal decrypted snapshot: %w", err)
				}
				snapshotState = decrypted
			}

			if err := snapshotState.UnmarshalTo(state); err != nil {
				return fmt.Errorf("failed to unmarshal snapshot state: %w", err)
			}

			replayFrom = snapshot.GetSequenceNumber() + 1
			entity.eventsCounter = snapshot.GetSequenceNumber()
		}
	}

	// Step 2: determine the latest event sequence number
	latestEvent, err := entity.eventsStore.GetLatestEvent(ctx, entity.persistenceID)
	if err != nil {
		return fmt.Errorf("failed to get latest event: %w", err)
	}

	if latestEvent == nil {
		entity.currentState = state
		return nil
	}

	latestSeqNr := latestEvent.GetSequenceNumber()

	// Step 3: replay events after the snapshot
	if replayFrom <= latestSeqNr {
		events, err := entity.eventsStore.ReplayEvents(ctx, entity.persistenceID, replayFrom, latestSeqNr, latestSeqNr-replayFrom+1)
		if err != nil {
			return fmt.Errorf("failed to replay events: %w", err)
		}

		for _, envelope := range events {
			evt := envelope.GetEvent()

			// Decrypt the event if it was encrypted
			if envelope.GetIsEncrypted() && entity.encryptor != nil {
				plaintext, err := entity.encryptor.Decrypt(ctx, entity.persistenceID, evt.GetValue(), envelope.GetEncryptionKeyId())
				if err != nil {
					return fmt.Errorf("failed to decrypt event at sequence %d: %w", envelope.GetSequenceNumber(), err)
				}

				decrypted := &anypb.Any{TypeUrl: evt.GetTypeUrl()}
				if err := proto.Unmarshal(plaintext, decrypted); err != nil {
					return fmt.Errorf("failed to unmarshal decrypted event at sequence %d: %w", envelope.GetSequenceNumber(), err)
				}
				evt = decrypted
			}

			// apply event adapters
			if len(entity.eventAdapters) > 0 {
				evt, err = eventadapter.Chain(entity.eventAdapters, evt, envelope.GetSequenceNumber())
				if err != nil {
					return fmt.Errorf("failed to adapt event at sequence %d: %w", envelope.GetSequenceNumber(), err)
				}
			}

			eventMsg, err := evt.UnmarshalNew()
			if err != nil {
				return fmt.Errorf("failed to unmarshal event at sequence %d: %w", envelope.GetSequenceNumber(), err)
			}

			state, err = entity.behavior.HandleEvent(ctx, eventMsg.(Event), state)
			if err != nil {
				return fmt.Errorf("failed to handle event at sequence %d: %w", envelope.GetSequenceNumber(), err)
			}
		}

		entity.eventsCounter = latestSeqNr
	}

	entity.currentState = state
	return nil
}

// sendErrorReply sends an error as a reply message
func (entity *EventSourcedActor) sendErrorReply(ctx *goakt.ReceiveContext, err error) {
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_ErrorReply{
			ErrorReply: &egopb.ErrorReply{
				Message: err.Error(),
			},
		},
	}

	ctx.Response(reply)
}

// getStateAndReply returns the current state of the entity.
// Returns the in-memory state directly since events no longer carry state.
func (entity *EventSourcedActor) getStateAndReply(ctx *goakt.ReceiveContext) {
	state, _ := anypb.New(entity.currentState)
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.persistenceID,
				State:          state,
				SequenceNumber: entity.eventsCounter,
				Timestamp:      entity.lastCommandTime.Unix(),
			},
		},
	}

	ctx.Response(reply)
}

// processCommandAndReply processes the incoming command
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

	// no-op when there are no events to process
	if len(events) == 0 {
		resultingState, _ := anypb.New(entity.currentState)
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  entity.persistenceID,
					State:          resultingState,
					SequenceNumber: entity.eventsCounter,
					Timestamp:      entity.lastCommandTime.Unix(),
				},
			},
		}
		ctx.Response(reply)
		return
	}

	shardNumber := ctx.ActorSystem().Partition(entity.persistenceID)
	topic := fmt.Sprintf(eventsTopic, shardNumber)

	var envelopes []*egopb.Event
	// process all events
	for _, event := range events {
		resultingState, err := entity.behavior.HandleEvent(goCtx, event, entity.currentState)
		if err != nil {
			entity.sendErrorReply(ctx, err)
			return
		}

		entity.eventsCounter++
		entity.currentState = resultingState
		entity.lastCommandTime = timestamppb.Now().AsTime()

		eventAny, _ := anypb.New(event)

		// Encrypt the event payload if an encryptor is configured
		var encKeyID string
		var isEncrypted bool
		if entity.encryptor != nil {
			eventBytes, err := proto.Marshal(eventAny)
			if err != nil {
				entity.sendErrorReply(ctx, fmt.Errorf("failed to marshal event for encryption: %w", err))
				return
			}
			ciphertext, keyID, err := entity.encryptor.Encrypt(goCtx, entity.persistenceID, eventBytes)
			if err != nil {
				entity.sendErrorReply(ctx, fmt.Errorf("failed to encrypt event: %w", err))
				return
			}
			eventAny = &anypb.Any{
				TypeUrl: eventAny.GetTypeUrl(),
				Value:   ciphertext,
			}
			encKeyID = keyID
			isEncrypted = true
		}

		// Events are pure — no state is stored on events.
		envelope := &egopb.Event{
			PersistenceId:   entity.persistenceID,
			SequenceNumber:  entity.eventsCounter,
			IsDeleted:       false,
			Event:           eventAny,
			Timestamp:       entity.lastCommandTime.Unix(),
			Shard:           uint64(shardNumber),
			EncryptionKeyId: encKeyID,
			IsEncrypted:     isEncrypted,
		}
		envelopes = append(envelopes, envelope)
	}

	ctx.Logger().Debugf("publishing events to topic: %s", topic)
	eg, goCtx := errgroup.WithContext(goCtx)
	eg.Go(func() error {
		for _, envelope := range envelopes {
			entity.eventsStream.Publish(topic, envelope)
		}
		return nil
	})

	eg.Go(func() error {
		return entity.eventsStore.WriteEvents(goCtx, envelopes)
	})

	// Persist snapshot if configured and interval is reached
	snapshotTaken := false
	if entity.snapshotStore != nil && entity.snapshotInterval > 0 && entity.eventsCounter%entity.snapshotInterval == 0 {
		snapshotTaken = true
		stateAny, _ := anypb.New(entity.currentState)

		var snapEncKeyID string
		var snapIsEncrypted bool
		if entity.encryptor != nil {
			stateBytes, err := proto.Marshal(stateAny)
			if err != nil {
				entity.sendErrorReply(ctx, fmt.Errorf("failed to marshal snapshot for encryption: %w", err))
				return
			}
			ciphertext, keyID, err := entity.encryptor.Encrypt(goCtx, entity.persistenceID, stateBytes)
			if err != nil {
				entity.sendErrorReply(ctx, fmt.Errorf("failed to encrypt snapshot: %w", err))
				return
			}
			stateAny = &anypb.Any{
				TypeUrl: stateAny.GetTypeUrl(),
				Value:   ciphertext,
			}
			snapEncKeyID = keyID
			snapIsEncrypted = true
		}

		snapshot := &egopb.Snapshot{
			PersistenceId:   entity.persistenceID,
			SequenceNumber:  entity.eventsCounter,
			State:           stateAny,
			Timestamp:       entity.lastCommandTime.Unix(),
			EncryptionKeyId: snapEncKeyID,
			IsEncrypted:     snapIsEncrypted,
		}
		eg.Go(func() error {
			return entity.snapshotStore.WriteSnapshot(goCtx, snapshot)
		})
	}

	if err := eg.Wait(); err != nil {
		entity.sendErrorReply(ctx, err)
		return
	}

	// Apply retention policy after a successful snapshot write
	if snapshotTaken && entity.retentionPolicy != nil {
		if entity.retentionPolicy.DeleteEventsOnSnapshot {
			deleteUpTo := entity.eventsCounter
			if entity.retentionPolicy.EventsRetentionCount > 0 && deleteUpTo > entity.retentionPolicy.EventsRetentionCount {
				deleteUpTo -= entity.retentionPolicy.EventsRetentionCount
			} else if entity.retentionPolicy.EventsRetentionCount > 0 {
				deleteUpTo = 0
			}
			if deleteUpTo > 0 {
				if err := entity.eventsStore.DeleteEvents(goCtx, entity.persistenceID, deleteUpTo); err != nil {
					ctx.Logger().Errorf("failed to delete events for retention policy: %s", err)
				}
			}
		}
		if entity.retentionPolicy.DeleteSnapshotsOnSnapshot && entity.eventsCounter > entity.snapshotInterval {
			previousSnapshotSeqNr := entity.eventsCounter - entity.snapshotInterval
			if err := entity.snapshotStore.DeleteSnapshots(goCtx, entity.persistenceID, previousSnapshotSeqNr); err != nil {
				ctx.Logger().Errorf("failed to delete old snapshots for retention policy: %s", err)
			}
		}
	}

	if entity.metrics != nil {
		entity.metrics.eventsPersisted.Add(goCtx, int64(len(envelopes)))
	}

	state, _ := anypb.New(entity.currentState)
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  entity.persistenceID,
				State:          state,
				SequenceNumber: entity.eventsCounter,
				Timestamp:      entity.lastCommandTime.Unix(),
			},
		},
	}

	ctx.Response(reply)
}
