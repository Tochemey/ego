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
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/tochemey/ego/v5/persistence"
)

const (
	// targetFlag names the flag holding the v5 database's connection string.
	targetFlag = "target"
	// countEventsQuery counts the events of a journal.
	countEventsQuery = `SELECT COUNT(*) FROM events_store`
	// latestSequenceNumbersQuery returns every persistence ID's latest sequence number.
	latestSequenceNumbersQuery = `SELECT persistence_id, MAX(sequence_number) FROM events_store GROUP BY persistence_id`
	// latestSnapshotsQuery returns the sequence number of every persistence ID's latest snapshot.
	latestSnapshotsQuery = `SELECT persistence_id, MAX(sequence_number) FROM snapshots_store GROUP BY persistence_id`
	// backwardStepsQuery counts the events stamped at or before their entity's previous event.
	backwardStepsQuery = `SELECT COUNT(*) FROM (
		SELECT timestamp - LAG(timestamp) OVER (PARTITION BY persistence_id ORDER BY sequence_number) AS step
		FROM events_store) steps WHERE step <= 0`
	// payloadDigestQuery digests every event payload, in journal order.
	payloadDigestQuery = `SELECT COALESCE(md5(string_agg(md5(event_payload), '' ORDER BY persistence_id, sequence_number)), '')
		FROM events_store`
	// entityShardsQuery returns the lowest and highest shard of every persistence ID's events.
	entityShardsQuery = `SELECT persistence_id, MIN(shard_number), MAX(shard_number) FROM events_store GROUP BY persistence_id`
	// legacyPartitionsQuery returns the v4 partitions that hold events.
	legacyPartitionsQuery = `SELECT DISTINCT shard_number FROM events_store`
	// legacyCommittedOffsetsQuery returns every offset the v4 projections committed.
	legacyCommittedOffsetsQuery = `SELECT projection_name, shard_number, current_offset FROM offsets_store`
)

// errVerificationFailed reports that the v5 data does not match the v4 data.
var errVerificationFailed = errors.New("the migrated data does not match the v4 data")

// newVerifyCommand returns the command that checks the v5 database against the v4 one.
func newVerifyCommand() *cobra.Command {
	var legacy, target string

	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Checks the v5 database against the v4 one",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			legacyPool, err := openPool(cmd.Context(), legacy)
			if err != nil {
				return err
			}
			defer legacyPool.Close()

			targetPool, err := openPool(cmd.Context(), target)
			if err != nil {
				return err
			}
			defer targetPool.Close()

			return verify(cmd, legacyPool, targetPool)
		},
	}

	cmd.Flags().StringVar(&legacy, legacyFlag, "", "connection string of the v4 database")
	cmd.Flags().StringVar(&target, targetFlag, "", "connection string of the v5 database")
	_ = cmd.MarkFlagRequired(legacyFlag)
	_ = cmd.MarkFlagRequired(targetFlag)
	return cmd
}

// check is the outcome of one verification.
type check struct {
	// name names what was checked.
	name string
	// passed reports whether the v5 data is right.
	passed bool
	// detail describes what was found.
	detail string
}

// verify runs every check, prints them and fails when any did not pass.
func verify(cmd *cobra.Command, legacy, target *pgxpool.Pool) error {
	ctx := cmd.Context()
	checks := []func(context.Context, *pgxpool.Pool, *pgxpool.Pool) (check, error){
		checkEvents,
		checkSequenceNumbers,
		checkPayloads,
		checkShards,
		checkTimestamps,
		checkSnapshots,
		checkProjections,
	}

	failed := false
	for _, run := range checks {
		result, err := run(ctx, legacy, target)
		if err != nil {
			return err
		}

		status := "PASS"
		if !result.passed {
			status = "FAIL"
			failed = true
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s  %-16s %s\n", status, result.name, result.detail)
	}

	if failed {
		return errVerificationFailed
	}
	return nil
}

// checkEvents compares the number of events of both journals.
func checkEvents(ctx context.Context, legacy, target *pgxpool.Pool) (check, error) {
	legacyCount, err := queryInt(ctx, legacy, countEventsQuery)
	if err != nil {
		return check{}, err
	}

	targetCount, err := queryInt(ctx, target, countEventsQuery)
	if err != nil {
		return check{}, err
	}

	return check{
		name:   "events",
		passed: legacyCount == targetCount,
		detail: fmt.Sprintf("%d in v4, %d in v5", legacyCount, targetCount),
	}, nil
}

// checkSequenceNumbers compares every entity's latest sequence number.
func checkSequenceNumbers(ctx context.Context, legacy, target *pgxpool.Pool) (check, error) {
	legacyLatest, err := queryCounts(ctx, legacy, latestSequenceNumbersQuery)
	if err != nil {
		return check{}, err
	}

	targetLatest, err := queryCounts(ctx, target, latestSequenceNumbersQuery)
	if err != nil {
		return check{}, err
	}

	return check{
		name:   "entities",
		passed: maps.Equal(legacyLatest, targetLatest),
		detail: fmt.Sprintf("%d entities in v4, %d in v5, with the same latest sequence numbers", len(legacyLatest), len(targetLatest)),
	}, nil
}

// checkPayloads compares a digest of every event payload of both journals.
func checkPayloads(ctx context.Context, legacy, target *pgxpool.Pool) (check, error) {
	var legacyDigest, targetDigest string
	if err := legacy.QueryRow(ctx, payloadDigestQuery).Scan(&legacyDigest); err != nil {
		return check{}, err
	}

	if err := target.QueryRow(ctx, payloadDigestQuery).Scan(&targetDigest); err != nil {
		return check{}, err
	}

	return check{
		name:   "payloads",
		passed: legacyDigest == targetDigest,
		detail: "every event payload is copied unchanged",
	}, nil
}

// checkShards verifies that every v5 event is on its entity's fixed shard.
func checkShards(ctx context.Context, legacy, target *pgxpool.Pool) (check, error) {
	legacyMoved, _, err := countShardMismatches(ctx, legacy)
	if err != nil {
		return check{}, err
	}

	targetMisplaced, entities, err := countShardMismatches(ctx, target)
	if err != nil {
		return check{}, err
	}

	return check{
		name:   "shards",
		passed: targetMisplaced == 0,
		detail: fmt.Sprintf("%d of %d entities were off persistence.ShardFor in v4, %d in v5", legacyMoved, entities, targetMisplaced),
	}, nil
}

// checkTimestamps verifies that every v5 entity's timestamps strictly increase.
func checkTimestamps(ctx context.Context, legacy, target *pgxpool.Pool) (check, error) {
	legacySteps, err := queryInt(ctx, legacy, backwardStepsQuery)
	if err != nil {
		return check{}, err
	}

	targetSteps, err := queryInt(ctx, target, backwardStepsQuery)
	if err != nil {
		return check{}, err
	}

	return check{
		name:   "timestamps",
		passed: targetSteps == 0,
		detail: fmt.Sprintf("%d events stamped at or before their previous one in v4, %d in v5", legacySteps, targetSteps),
	}, nil
}

// checkSnapshots compares every entity's latest snapshot, including the
// entities whose events retention deleted.
func checkSnapshots(ctx context.Context, legacy, target *pgxpool.Pool) (check, error) {
	legacyLatest, err := queryCounts(ctx, legacy, latestSnapshotsQuery)
	if err != nil {
		return check{}, err
	}

	targetLatest, err := queryCounts(ctx, target, latestSnapshotsQuery)
	if err != nil {
		return check{}, err
	}

	journal, err := queryCounts(ctx, legacy, latestSequenceNumbersQuery)
	if err != nil {
		return check{}, err
	}

	snapshotOnly := 0
	for persistenceID := range legacyLatest {
		if _, ok := journal[persistenceID]; !ok {
			snapshotOnly++
		}
	}

	return check{
		name:   "snapshots",
		passed: maps.Equal(legacyLatest, targetLatest),
		detail: fmt.Sprintf("latest snapshot of %d entities copied, %d of them with no event left", len(targetLatest), snapshotOnly),
	}, nil
}

// checkProjections verifies that every v4 projection resumes from its oldest
// offset, or from the start when a v4 partition holding events has no offset of
// it, with no per-entity offset yet. The expected starts are computed from the
// raw v4 rows, apart from the reader the migration used.
func checkProjections(ctx context.Context, legacy, target *pgxpool.Pool) (check, error) {
	partitions, err := queryPartitions(ctx, legacy)
	if err != nil {
		return check{}, err
	}

	committed, err := queryLegacyOffsets(ctx, legacy)
	if err != nil {
		return check{}, err
	}

	starts, err := queryCounts(ctx, target, `SELECT projection_name, start_offset FROM offsets_start`)
	if err != nil {
		return check{}, err
	}

	rows, err := queryInt(ctx, target, `SELECT COUNT(*) FROM offsets_store`)
	if err != nil {
		return check{}, err
	}

	expected := make(map[string]int64, len(committed))
	resumed := make([]string, 0, len(committed))
	for _, projectionName := range slices.Sorted(maps.Keys(committed)) {
		start, uncommitted := expectedStart(partitions, committed[projectionName])
		expected[projectionName] = start

		if uncommitted > 0 {
			resumed = append(resumed, fmt.Sprintf("%s from the start (v4 partitions holding events without its offset: %d)", projectionName, uncommitted))
			continue
		}

		resumed = append(resumed, projectionName+" from its oldest v4 offset")
	}

	return check{
		name:   "projections",
		passed: maps.Equal(expected, starts) && rows == 0,
		detail: strings.Join(resumed, "; "),
	}, nil
}

// countShardMismatches returns how many entities have an event off
// persistence.ShardFor, and how many entities the journal has.
func countShardMismatches(ctx context.Context, pool *pgxpool.Pool) (mismatches, entities int, err error) {
	rows, err := pool.Query(ctx, entityShardsQuery)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			persistenceID string
			lowest        uint64
			highest       uint64
		)
		if err := rows.Scan(&persistenceID, &lowest, &highest); err != nil {
			return 0, 0, err
		}

		entities++
		shard := persistence.ShardFor(persistenceID)
		if lowest != shard || highest != shard {
			mismatches++
		}
	}
	return mismatches, entities, rows.Err()
}

// queryInt runs a query returning one integer.
func queryInt(ctx context.Context, pool *pgxpool.Pool, query string) (int64, error) {
	var value int64
	err := pool.QueryRow(ctx, query).Scan(&value)
	return value, err
}

// queryCounts runs a query returning a name and an integer per row.
func queryCounts(ctx context.Context, pool *pgxpool.Pool, query string) (map[string]int64, error) {
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var (
			name  string
			value int64
		)
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		counts[name] = value
	}
	return counts, rows.Err()
}

// queryPartitions returns the v4 partitions that hold events.
func queryPartitions(ctx context.Context, pool *pgxpool.Pool) ([]int64, error) {
	rows, err := pool.Query(ctx, legacyPartitionsQuery)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// queryLegacyOffsets returns every offset the v4 projections committed, by
// projection and partition.
func queryLegacyOffsets(ctx context.Context, pool *pgxpool.Pool) (map[string]map[int64]int64, error) {
	rows, err := pool.Query(ctx, legacyCommittedOffsetsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	offsets := make(map[string]map[int64]int64)
	for rows.Next() {
		var (
			projectionName string
			partition      int64
			offset         int64
		)
		if err := rows.Scan(&projectionName, &partition, &offset); err != nil {
			return nil, err
		}

		if offsets[projectionName] == nil {
			offsets[projectionName] = make(map[int64]int64)
		}
		offsets[projectionName][partition] = offset
	}
	return offsets, rows.Err()
}

// expectedStart returns the point a v4 projection resumes from on the v5
// journal, given the v4 partitions holding events and the offsets it committed
// per partition: its oldest offset on those partitions, or zero, from the
// start, when it committed none on some of them. It also returns how many of
// them it committed no offset on.
func expectedStart(partitions []int64, offsets map[int64]int64) (start int64, uncommitted int) {
	start = math.MaxInt64
	for _, partition := range partitions {
		offset, ok := offsets[partition]
		if !ok {
			uncommitted++
			continue
		}

		start = min(start, offset)
	}

	if uncommitted > 0 {
		return 0, uncommitted
	}

	return start, 0
}
