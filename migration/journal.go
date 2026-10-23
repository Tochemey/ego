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

package migration

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

// JournalReader is the part of an events store a JournalMigrator reads from.
// Every persistence.EventsStore implements it, so the source of a migration is
// usually an events store configured over the journal to migrate.
type JournalReader interface {
	// Ping verifies a connection to the database is still alive.
	Ping(ctx context.Context) error
	// PersistenceIDs returns a page of the persistence IDs in the journal, in
	// a stable order, starting after pageToken. Pages must not overlap, or an
	// entity is migrated twice at once.
	PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error)
	// GetLatestEvent returns the latest event of the persistence ID, or nil.
	GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error)
	// ReplayEvents returns at most limit events of the persistence ID between
	// the two sequence numbers, both inclusive, in sequence order.
	ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, limit uint64) ([]*egopb.Event, error)
}

// JournalMigrator copies a journal into an events store in the layout eGo
// reads it with: every event on the shard persistence.ShardFor gives its
// persistence ID, and the timestamps of every persistence ID increasing.
//
// Journals written by earlier releases stored each event on the cluster
// partition of its entity, which changed with the cluster's partition count,
// and could stamp an entity's event before its previous one when a clock went
// back. The migrator recomputes the shard of every event and moves any such
// timestamp to a nanosecond after the previous event of its persistence ID.
// Everything else is copied unchanged.
//
// The migration is resumable: an entity whose events are partly copied already
// continues after the last one the target holds. It can therefore run while
// the legacy deployment still writes; once every writer is stopped, one more
// run copies the remainder. Turn the legacy deployment's events retention off
// until the last run: events it deletes after a run, beyond what the target
// holds, are missing from the target journal, and a projection stops at such a
// gap.
type JournalMigrator struct {
	settings
	source JournalReader
	target persistence.EventsStore
}

// NewJournalMigrator creates a JournalMigrator copying the journal of source
// into target.
func NewJournalMigrator(source JournalReader, target persistence.EventsStore, opts ...Option) *JournalMigrator {
	migrator := &JournalMigrator{
		settings: defaultSettings(),
		source:   source,
		target:   target,
	}

	for _, opt := range opts {
		opt.apply(&migrator.settings)
	}

	return migrator
}

// Run copies every persistence ID of the source journal into the target
// events store, several entities at a time (see WithParallelism).
func (m *JournalMigrator) Run(ctx context.Context) error {
	if err := m.source.Ping(ctx); err != nil {
		return fmt.Errorf("migration: source journal not reachable: %w", err)
	}

	if err := m.target.Ping(ctx); err != nil {
		return fmt.Errorf("migration: target events store not reachable: %w", err)
	}

	total, err := forEachEntity(ctx, m.source, m.settings, func(ctx context.Context, persistenceID string) error {
		if err := m.copyEntity(ctx, persistenceID); err != nil {
			return fmt.Errorf("failed to copy the journal of %s: %w", persistenceID, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("migration: %w", err)
	}

	m.logger.Infof("migration: copied the journal of %d entities", total)
	return nil
}

// copyEntity copies the events of one persistence ID that the target does not
// hold yet, a page at a time.
func (m *JournalMigrator) copyEntity(ctx context.Context, persistenceID string) error {
	latest, err := m.source.GetLatestEvent(ctx, persistenceID)
	if err != nil || latest == nil {
		return err
	}

	copied, err := m.target.GetLatestEvent(ctx, persistenceID)
	if err != nil {
		return err
	}

	from := uint64(1)
	previousTimestamp := int64(0)
	if copied != nil {
		from = copied.GetSequenceNumber() + 1
		previousTimestamp = copied.GetTimestamp()
	}

	shard := persistence.ShardFor(persistenceID)
	for from <= latest.GetSequenceNumber() {
		events, err := m.source.ReplayEvents(ctx, persistenceID, from, latest.GetSequenceNumber(), m.pageSize)
		if err != nil {
			return err
		}

		if len(events) == 0 {
			return nil
		}

		page := make([]*egopb.Event, 0, len(events))
		for _, event := range events {
			migrated := proto.CloneOf(event)
			migrated.Shard = shard
			migrated.Timestamp = max(event.GetTimestamp(), previousTimestamp+1)
			previousTimestamp = migrated.GetTimestamp()
			page = append(page, migrated)
		}

		if err := m.target.WriteEvents(ctx, page); err != nil {
			return err
		}

		from = page[len(page)-1].GetSequenceNumber() + 1
	}

	return nil
}
