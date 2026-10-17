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

// Package migration provides utilities for migrating the data of earlier eGo
// releases. The command in github.com/tochemey/ego/cmd/migrate runs them
// from the command line.
//
// The JournalMigrator copies a journal into an events store in the layout eGo
// reads it with: every event on the shard of its persistence ID, with the
// timestamps of every persistence ID increasing. The SnapshotMigrator copies
// the latest snapshot of every entity. ResumeProjections then makes every
// projection resume on the migrated journal:
//
//	if err := migration.NewJournalMigrator(legacyEventsStore, eventsStore).Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	if err := migration.NewSnapshotMigrator(legacyEventsStore, legacySnapshotStore, snapshotStore).Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	if _, err := migration.ResumeProjections(ctx, offsetStore, legacyOffsetReader); err != nil {
//	    log.Fatal(err)
//	}
//
// Every migrator reads the persistence IDs one page at a time and migrates
// several entities at once (see WithPageSize and WithParallelism), so its
// memory does not grow with the size of the stores, and every migrator can be
// stopped and run again.
//
// The Migrator moves event-sourced entities from the legacy format (where
// events carried resulting_state inline) to the format where snapshots are
// stored separately in a SnapshotStore.
//
// Usage:
//
//	migrator := migration.New(eventsStore, snapshotStore,
//	    migration.WithPageSize(100),
//	    migration.WithLogger(logger),
//	)
//	if err := migrator.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
// The migrator reads all persistence IDs from the events store, finds the latest
// event for each entity that carried a resulting_state (field 5 in the old proto),
// and writes a snapshot to the snapshot store seeded from that state. This is a
// one-time, idempotent operation — running it again will overwrite existing snapshots
// with the same data.
package migration

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

// legacyResultingStateFieldNumber is the protobuf field number that was used
// for resulting_state in the old Event message. We decode it directly from
// the raw event bytes to avoid depending on a generated LegacyEvent type.
const legacyResultingStateFieldNumber = 5

// Migrator reads legacy events that embedded resulting_state and seeds
// the snapshot store with the latest state for each entity.
type Migrator struct {
	settings
	eventsStore   JournalReader
	snapshotStore persistence.SnapshotStore
}

// New creates a Migrator reading the legacy events of eventsStore, which every
// persistence.EventsStore can be, and writing snapshots to snapshotStore.
func New(eventsStore JournalReader, snapshotStore persistence.SnapshotStore, opts ...Option) *Migrator {
	m := &Migrator{
		settings:      defaultSettings(),
		eventsStore:   eventsStore,
		snapshotStore: snapshotStore,
	}
	for _, opt := range opts {
		opt.apply(&m.settings)
	}
	return m
}

// Run executes the migration. It iterates over all persistence IDs in the events
// store, several entities at a time (see WithParallelism), and, for each
// entity, extracts the resulting_state from the latest event that carried one,
// writing it as a snapshot.
//
// The operation is idempotent: running it multiple times produces the same result.
// Events in the store are not modified.
func (m *Migrator) Run(ctx context.Context) error {
	if err := m.eventsStore.Ping(ctx); err != nil {
		return fmt.Errorf("migration: events store not reachable: %w", err)
	}
	if err := m.snapshotStore.Ping(ctx); err != nil {
		return fmt.Errorf("migration: snapshot store not reachable: %w", err)
	}

	total, err := forEachEntity(ctx, m.eventsStore, m.settings, func(ctx context.Context, persistenceID string) error {
		if err := m.migrateEntity(ctx, persistenceID); err != nil {
			return fmt.Errorf("failed to migrate entity %s: %w", persistenceID, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("migration: %w", err)
	}

	m.logger.Infof("migration: completed successfully, processed %d entities", total)
	return nil
}

// migrateEntity processes a single entity: reads its events a page at a time
// and finds the latest one with a resulting_state, then writes a snapshot.
func (m *Migrator) migrateEntity(ctx context.Context, persistenceID string) error {
	latest, err := m.eventsStore.GetLatestEvent(ctx, persistenceID)
	if err != nil || latest == nil {
		return err
	}

	var bestSnapshot *egopb.Snapshot
	for from := uint64(1); from <= latest.GetSequenceNumber(); {
		events, err := m.eventsStore.ReplayEvents(ctx, persistenceID, from, latest.GetSequenceNumber(), m.pageSize)
		if err != nil {
			return err
		}

		if len(events) == 0 {
			break
		}

		for _, evt := range events {
			if state := extractLegacyResultingState(evt); state != nil {
				bestSnapshot = &egopb.Snapshot{
					PersistenceId:  persistenceID,
					SequenceNumber: evt.GetSequenceNumber(),
					State:          state,
					Timestamp:      evt.GetTimestamp(),
				}
			}
		}

		from = events[len(events)-1].GetSequenceNumber() + 1
	}

	if bestSnapshot == nil {
		m.logger.Debugf("migration: entity %s has no legacy resulting_state, skipping", persistenceID)
		return nil
	}

	if err := m.snapshotStore.WriteSnapshot(ctx, bestSnapshot); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	m.logger.Debugf("migration: entity %s snapshot written at sequence %d", persistenceID, bestSnapshot.GetSequenceNumber())
	return nil
}

// extractLegacyResultingState attempts to read the resulting_state (field 5)
// from an event. Since the field was removed from the proto, it lands in the
// event's proto unknown fields when deserialized by the current generated code.
// We extract it by re-reading the unknown fields.
//
// The function uses the Event's proto unknown fields to find field 5 (the old
// resulting_state). If found, it unmarshals it as an anypb.Any.
func extractLegacyResultingState(evt *egopb.Event) *anypb.Any {
	if evt == nil {
		return nil
	}

	// When the current generated code deserializes an old event that has field 5,
	// it places the bytes into the message's unknown fields. We can scan them.
	raw := evt.ProtoReflect().GetUnknown()
	if len(raw) == 0 {
		return nil
	}

	// Parse the unknown fields looking for field number 5 (length-delimited, wire type 2)
	for len(raw) > 0 {
		fieldNum, wireType, n := consumeTag(raw)
		if n < 0 {
			break
		}
		raw = raw[n:]

		if wireType == 2 { // length-delimited
			length, n := consumeVarint(raw)
			if n < 0 {
				break
			}
			raw = raw[n:]

			if uint64(len(raw)) < length {
				break
			}

			if fieldNum == legacyResultingStateFieldNumber {
				state := new(anypb.Any)
				if err := proto.Unmarshal(raw[:length], state); err != nil {
					return nil
				}
				if state.GetTypeUrl() != "" {
					return state
				}
				return nil
			}

			raw = raw[length:]
		} else if wireType == 0 { // varint
			_, n := consumeVarint(raw)
			if n < 0 {
				break
			}
			raw = raw[n:]
		} else if wireType == 5 { // 32-bit
			if len(raw) < 4 {
				break
			}
			raw = raw[4:]
		} else if wireType == 1 { // 64-bit
			if len(raw) < 8 {
				break
			}
			raw = raw[8:]
		} else {
			break // unknown wire type
		}
	}
	return nil
}

// consumeTag parses a protobuf tag (field number + wire type) from raw bytes.
// Returns field number, wire type, and number of bytes consumed. Returns -1 for n on error.
func consumeTag(b []byte) (fieldNum uint32, wireType int, n int) {
	v, n := consumeVarint(b)
	if n < 0 {
		return 0, 0, -1
	}
	return uint32(v >> 3), int(v & 0x7), n
}

// consumeVarint parses a protobuf varint from raw bytes.
// Returns the value and number of bytes consumed. Returns -1 for n on error.
func consumeVarint(b []byte) (uint64, int) {
	var v uint64
	for i, c := range b {
		if i >= 10 {
			return 0, -1
		}
		v |= uint64(c&0x7f) << (uint(i) * 7)
		if c < 0x80 {
			return v, i + 1
		}
	}
	return 0, -1
}
