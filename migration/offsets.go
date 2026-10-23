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
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/tochemey/ego/v5/offsetstore"
)

// ErrProjectionStarted is returned when a projection to resume recorded offsets
// on the migrated journal already: resuming it would delete them and deliver
// its events again.
var ErrProjectionStarted = errors.New("migration: the projection recorded offsets on the migrated journal already")

// LegacyOffsetReader reads the offsets projections committed on a legacy
// offset store, which kept one offset per projection and shard.
type LegacyOffsetReader interface {
	// LegacyOffsets returns, for every projection, its offset on every legacy
	// shard that holds events. A shard the projection committed no offset on is
	// reported as zero: the legacy projection read such a shard from its start,
	// or from its Options.StartOffset when it had one. The migrated projection
	// applies the same Options.StartOffset, so keep it unchanged. SQL
	// stores whose legacy journal and offsets share a database implement it with
	// a single query:
	//
	//	SELECT p.projection_name, COALESCE(o.current_offset, 0)
	//	FROM (SELECT DISTINCT projection_name FROM offsets_store) AS p
	//	CROSS JOIN (SELECT DISTINCT shard_number FROM events_store) AS s
	//	LEFT JOIN offsets_store AS o
	//	  ON o.projection_name = p.projection_name AND o.shard_number = s.shard_number
	LegacyOffsets(ctx context.Context) (map[string][]int64, error)
}

// ResumeProjection makes the named projection resume, on the migrated journal,
// from the oldest of its offsets on the legacy journal, one per legacy shard
// that holds events, zero for a shard it committed no offset on (see
// LegacyOffsetReader). The legacy offsets do not map onto the new shards, so
// starting from the oldest of them skips nothing; the projection delivers
// again the events after that point it had processed already, as
// at-least-once delivery allows. A projection without legacy offsets starts
// from the beginning of the journal.
//
// Resuming a projection deletes every offset it holds (see
// offsetstore.OffsetStore.ResetOffset). Resume the projections after the
// legacy writers are stopped and before the migrated deployment starts them.
// A projection that recorded offsets on the migrated journal already is not
// resumed: ResumeProjection returns ErrProjectionStarted and keeps its offsets.
// Until then it can run again.
func ResumeProjection(ctx context.Context, offsetStore offsetstore.OffsetStore, projectionName string, legacyOffsets []int64) error {
	positions, err := offsetStore.LatestOffsets(ctx, projectionName)
	if err != nil {
		return fmt.Errorf("migration: failed to read the offsets of projection %s: %w", projectionName, err)
	}

	if len(positions) > 0 {
		return fmt.Errorf("%w: %s", ErrProjectionStarted, projectionName)
	}

	start := int64(0)
	if len(legacyOffsets) > 0 {
		start = slices.Min(legacyOffsets)
	}

	if err := offsetStore.ResetOffset(ctx, projectionName, start); err != nil {
		return fmt.Errorf("migration: failed to resume projection %s: %w", projectionName, err)
	}

	return nil
}

// ResumeProjections makes every projection the legacy offset store knows
// resume on the migrated journal (see ResumeProjection), in the order of their
// names, and returns how many it resumed.
func ResumeProjections(ctx context.Context, offsetStore offsetstore.OffsetStore, legacy LegacyOffsetReader) (int, error) {
	offsets, err := legacy.LegacyOffsets(ctx)
	if err != nil {
		return 0, fmt.Errorf("migration: failed to read the legacy offsets: %w", err)
	}

	projectionNames := slices.Sorted(maps.Keys(offsets))
	for resumed, projectionName := range projectionNames {
		if err := ResumeProjection(ctx, offsetStore, projectionName, offsets[projectionName]); err != nil {
			return resumed, err
		}
	}

	return len(projectionNames), nil
}
