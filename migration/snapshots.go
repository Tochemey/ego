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

// SnapshotReader is the part of a snapshot store a SnapshotMigrator reads
// from: a persistence.SnapshotStore that can also list its persistence IDs.
//
// The migrator lists the snapshot store rather than the journal: an entity
// whose events were all deleted by its retention policy has a snapshot and no
// event left, and must be migrated too.
type SnapshotReader interface {
	// Ping verifies a connection to the database is still alive.
	Ping(ctx context.Context) error
	// PersistenceIDs returns a page of the persistence IDs that have a
	// snapshot, in a stable order, starting after pageToken; pages must not
	// overlap. SQL-backed stores implement it with a single query:
	//
	//	SELECT DISTINCT persistence_id FROM snapshots_store
	//	WHERE persistence_id > $1 ORDER BY persistence_id LIMIT $2
	PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error)
	// GetLatestSnapshot returns the latest snapshot of the persistence ID, or nil.
	GetLatestSnapshot(ctx context.Context, persistenceID string) (*egopb.Snapshot, error)
}

// SnapshotMigrator copies the latest snapshot of every entity of a snapshot
// store into another one. An entity recovers from its latest snapshot only, so
// older snapshots are not copied.
//
// The migration is resumable: an entity whose target snapshot is as recent as
// its source snapshot is skipped. It can therefore run while the legacy
// deployment still writes; once every writer is stopped, one more run copies
// the remainder.
type SnapshotMigrator struct {
	settings
	source SnapshotReader
	target persistence.SnapshotStore
}

// NewSnapshotMigrator creates a SnapshotMigrator copying the latest snapshot
// of every persistence ID of source into target.
func NewSnapshotMigrator(source SnapshotReader, target persistence.SnapshotStore, opts ...Option) *SnapshotMigrator {
	migrator := &SnapshotMigrator{
		settings: defaultSettings(),
		source:   source,
		target:   target,
	}

	for _, opt := range opts {
		opt.apply(&migrator.settings)
	}

	return migrator
}

// Run copies the latest snapshot of every persistence ID of the source
// snapshot store, several entities at a time (see WithParallelism).
func (m *SnapshotMigrator) Run(ctx context.Context) error {
	if err := m.source.Ping(ctx); err != nil {
		return fmt.Errorf("migration: source snapshot store not reachable: %w", err)
	}

	if err := m.target.Ping(ctx); err != nil {
		return fmt.Errorf("migration: target snapshot store not reachable: %w", err)
	}

	total, err := forEachEntity(ctx, m.source, m.settings, func(ctx context.Context, persistenceID string) error {
		if err := m.copySnapshot(ctx, persistenceID); err != nil {
			return fmt.Errorf("failed to copy the snapshot of %s: %w", persistenceID, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("migration: %w", err)
	}

	m.logger.Infof("migration: copied the snapshots of %d entities", total)
	return nil
}

// copySnapshot copies the latest snapshot of the persistence ID, unless the
// target holds one as recent.
func (m *SnapshotMigrator) copySnapshot(ctx context.Context, persistenceID string) error {
	latest, err := m.source.GetLatestSnapshot(ctx, persistenceID)
	if err != nil || latest == nil {
		return err
	}

	copied, err := m.target.GetLatestSnapshot(ctx, persistenceID)
	if err != nil {
		return err
	}

	if copied != nil && copied.GetSequenceNumber() >= latest.GetSequenceNumber() {
		return nil
	}

	return m.target.WriteSnapshot(ctx, proto.CloneOf(latest))
}
