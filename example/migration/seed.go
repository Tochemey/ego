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

package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	samplepb "github.com/tochemey/ego/v5/example/examplepb"
)

const (
	// legacyFlag names the flag holding the v4 database's connection string.
	legacyFlag = "legacy"
	// entitiesFlag names the flag holding how many entities the v4 journal has.
	entitiesFlag = "entities"
	// eventsFlag names the flag holding how many events each entity has.
	eventsFlag = "events"
	// defaultEntities is how many entities the v4 journal has by default.
	defaultEntities = 10_000
	// defaultEventsPerEntity is how many events each entity has by default.
	defaultEventsPerEntity = 100
	// legacyPartitionCount is the partition count of the example's v4 cluster:
	// a v4 event's shard was its entity's cluster partition.
	legacyPartitionCount = 271
	// clockStepBackEvery selects the entities, one in this many, whose clock
	// stepped back half way through their journal.
	clockStepBackEvery = 10
	// clockStepBack is how far those clocks stepped back.
	clockStepBack = 10 * time.Second
	// retentionEvery selects the entities, one in this many, whose retention
	// policy deleted every event up to their latest snapshot.
	retentionEvery = 25
	// eventSpacing is the time between two events of an entity.
	eventSpacing = time.Millisecond
	// seedHistory is how long ago the v4 journal starts.
	seedHistory = 30 * 24 * time.Hour
	// copyChunkRows is how many rows one COPY into the v4 database sends.
	copyChunkRows = 50_000
	// openingBalance is the balance every account is created with.
	openingBalance = 100
	// balancesProjection and auditProjection are the v4 projections whose
	// offsets the migration resumes.
	balancesProjection = "balances"
	auditProjection    = "audit"
)

// eventsCopyColumns lists the events_store columns the seed copies, in row order.
var eventsCopyColumns = []string{"persistence_id", "sequence_number", "is_deleted", "event_payload", "event_manifest",
	"timestamp", "shard_number", "encryption_key_id", "is_encrypted"}

// newSeedCommand returns the command that fills the v4 database.
func newSeedCommand() *cobra.Command {
	var (
		legacy   string
		entities int
		events   int
	)

	cmd := &cobra.Command{
		Use:   "seed",
		Short: "Fills the v4 database with a large journal, snapshots and projection offsets",
		Long: "Replaces the v4 data with a journal of --entities entities of --events events each, stamped on the " +
			"entities' v4 cluster partitions. One entity in ten has a clock that stepped back half way through its " +
			"journal, one in twenty-five lost every event to its retention policy and keeps only a snapshot, and two " +
			"projections committed one offset per v4 partition.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pool, err := openPool(cmd.Context(), legacy)
			if err != nil {
				return err
			}
			defer pool.Close()

			return seed(cmd, pool, entities, events)
		},
	}

	cmd.Flags().StringVar(&legacy, legacyFlag, "", "connection string of the v4 database")
	cmd.Flags().IntVar(&entities, entitiesFlag, defaultEntities, "number of entities")
	cmd.Flags().IntVar(&events, eventsFlag, defaultEventsPerEntity, "number of events per entity")
	_ = cmd.MarkFlagRequired(legacyFlag)
	return cmd
}

// seed replaces the v4 data with the generated journal, snapshots and offsets.
func seed(cmd *cobra.Command, pool *pgxpool.Pool, entities, eventsPerEntity int) error {
	ctx := cmd.Context()
	started := time.Now()

	if _, err := pool.Exec(ctx, `TRUNCATE events_store, snapshots_store, offsets_store`); err != nil {
		return fmt.Errorf("clear the v4 tables: %w", err)
	}

	base := time.Now().Add(-seedHistory)
	rows := make([][]any, 0, copyChunkRows)
	written := 0

	for index := 1; index <= entities; index++ {
		persistenceID := entityID(index)
		if err := writeSnapshots(ctx, pool, index, eventsPerEntity, base); err != nil {
			return err
		}

		if isRetentionEntity(index) {
			continue
		}

		created, credited, err := entityPayloads(persistenceID)
		if err != nil {
			return err
		}

		for sequenceNumber := 1; sequenceNumber <= eventsPerEntity; sequenceNumber++ {
			payload := credited
			if sequenceNumber == 1 {
				payload = created
			}

			rows = append(rows, []any{persistenceID, int64(sequenceNumber), false, payload.bytes, payload.manifest,
				eventTimestamp(index, sequenceNumber, eventsPerEntity, base), int64(legacyShard(persistenceID)), "", false})
		}

		if len(rows) >= copyChunkRows {
			if err := copyEvents(ctx, pool, rows); err != nil {
				return err
			}
			written += len(rows)
			rows = rows[:0]
		}
	}

	if err := copyEvents(ctx, pool, rows); err != nil {
		return err
	}
	written += len(rows)

	if err := writeLegacyOffsets(ctx, pool, base); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "seeded %d events of %d entities, their snapshots and the offsets of 2 projections in %s\n",
		written, entities, time.Since(started).Round(time.Millisecond))
	return nil
}

// eventPayload is an event as the v4 store saved it: the marshaled Any and its type URL.
type eventPayload struct {
	// bytes is the marshaled google.protobuf.Any.
	bytes []byte
	// manifest is the Any's type URL.
	manifest string
}

// entityPayloads returns the creation and credit events of the account.
func entityPayloads(persistenceID string) (created, credited eventPayload, err error) {
	created, err = marshalEvent(&samplepb.AccountCreated{AccountId: persistenceID, AccountBalance: openingBalance})
	if err != nil {
		return created, credited, err
	}

	credited, err = marshalEvent(&samplepb.AccountCredited{AccountId: persistenceID, AccountBalance: 1})
	return created, credited, err
}

// marshalEvent encodes an event the way the v4 store did.
func marshalEvent(event proto.Message) (eventPayload, error) {
	wrapped, err := anypb.New(event)
	if err != nil {
		return eventPayload{}, err
	}

	bytes, err := proto.Marshal(wrapped)
	if err != nil {
		return eventPayload{}, err
	}
	return eventPayload{bytes: bytes, manifest: wrapped.GetTypeUrl()}, nil
}

// writeSnapshots stores the entity's snapshots: two for a regular entity, the
// latest at half its journal, and one at its last event for an entity whose
// events retention deleted.
func writeSnapshots(ctx context.Context, pool *pgxpool.Pool, index, eventsPerEntity int, base time.Time) error {
	sequenceNumbers := []int{max(eventsPerEntity/4, 1), max(eventsPerEntity/2, 1)}
	if isRetentionEntity(index) {
		sequenceNumbers = []int{eventsPerEntity}
	}

	persistenceID := entityID(index)
	batch := &pgx.Batch{}
	for _, sequenceNumber := range sequenceNumbers {
		state, err := marshalEvent(&samplepb.Account{AccountId: persistenceID, AccountBalance: float64(openingBalance + sequenceNumber - 1)})
		if err != nil {
			return err
		}

		batch.Queue(`INSERT INTO snapshots_store (persistence_id, sequence_number, state_payload, state_manifest, timestamp)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
			persistenceID, sequenceNumber, state.bytes, state.manifest, eventTimestamp(index, sequenceNumber, eventsPerEntity, base))
	}

	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("write the snapshots of %s: %w", persistenceID, err)
	}
	return nil
}

// writeLegacyOffsets stores, for both projections, one committed offset per v4
// partition. The audit projection is ten days behind the balances one.
func writeLegacyOffsets(ctx context.Context, pool *pgxpool.Pool, base time.Time) error {
	committedAt := time.Now().UnixNano()
	positions := map[string]time.Time{
		balancesProjection: base.Add(20 * 24 * time.Hour),
		auditProjection:    base.Add(10 * 24 * time.Hour),
	}

	batch := &pgx.Batch{}
	for projectionName, position := range positions {
		for partition := range legacyPartitionCount {
			batch.Queue(`INSERT INTO offsets_store (projection_name, shard_number, current_offset, timestamp) VALUES ($1, $2, $3, $4)`,
				projectionName, partition, position.Add(time.Duration(partition)*time.Millisecond).UnixNano(), committedAt)
		}
	}

	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("write the legacy offsets: %w", err)
	}
	return nil
}

// copyEvents sends the event rows to the v4 journal with one COPY.
func copyEvents(ctx context.Context, pool *pgxpool.Pool, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"events_store"}, eventsCopyColumns, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy events: %w", err)
	}
	return nil
}

// entityID returns the persistence ID of the entity with the given index.
func entityID(index int) string {
	return fmt.Sprintf("account-%07d", index)
}

// legacyShard returns the v4 shard of the persistence ID: its partition in the
// example's v4 cluster.
func legacyShard(persistenceID string) uint32 {
	return crc32.ChecksumIEEE([]byte(persistenceID)) % legacyPartitionCount
}

// isRetentionEntity reports whether the entity's retention policy deleted every event it had.
func isRetentionEntity(index int) bool {
	return index%retentionEvery == 0
}

// eventTimestamp returns the v4 timestamp of an entity's event. The entity's
// events are a millisecond apart, except for the entities whose clock stepped
// back: their second half is stamped clockStepBack earlier.
func eventTimestamp(index, sequenceNumber, eventsPerEntity int, base time.Time) int64 {
	stamp := base.Add(time.Duration(index) * time.Microsecond).Add(time.Duration(sequenceNumber-1) * eventSpacing)
	if index%clockStepBackEvery == 0 && sequenceNumber > eventsPerEntity/2 {
		stamp = stamp.Add(-clockStepBack)
	}
	return stamp.UnixNano()
}
