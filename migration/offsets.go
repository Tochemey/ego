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
	"maps"
	"slices"

	"github.com/tochemey/ego/v5/offsetstore"
)

// LegacyOffsetReader reads the offsets projections committed on a legacy
// offset store, which kept one offset per projection and shard.
type LegacyOffsetReader interface {
	// LegacyOffsets returns, for every projection, the offsets it committed,
	// one per legacy shard. SQL-backed stores implement it with a single query:
	//
	//	SELECT projection_name, current_offset FROM offsets_store
	LegacyOffsets(ctx context.Context) (map[string][]int64, error)
}

// ResumeProjection makes the named projection resume, on the migrated journal,
// from the oldest of the offsets it had committed on the legacy journal, one per
// legacy shard. The legacy offsets do not map onto the new shards, so starting
// from the oldest of them skips nothing; the projection delivers again the
// events after that point it had processed already, as at-least-once delivery
// allows. A projection without legacy offsets starts from the beginning of the
// journal.
//
// Resuming a projection deletes every offset it holds (see
// offsetstore.OffsetStore.ResetOffset). Resume the projections once, after the
// legacy writers are stopped and before the migrated deployment starts them.
func ResumeProjection(ctx context.Context, offsetStore offsetstore.OffsetStore, projectionName string, legacyOffsets []int64) error {
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
