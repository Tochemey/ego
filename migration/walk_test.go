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
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pagedLister is a persistenceIDLister over a fixed list, returning pages of
// the requested size with the last persistence ID of a page as its token.
type pagedLister struct {
	// persistenceIDs is the sorted list the lister pages through.
	persistenceIDs []string
	// failAt makes the listing fail for this page token, when set.
	failAt string
}

// PersistenceIDs returns the page after pageToken.
func (x pagedLister) PersistenceIDs(_ context.Context, pageSize uint64, pageToken string) ([]string, string, error) {
	if x.failAt != "" && pageToken == x.failAt {
		return nil, "", assert.AnError
	}

	start := 0
	if pageToken != "" {
		start = slices.Index(x.persistenceIDs, pageToken) + 1
	}

	end := min(start+int(pageSize), len(x.persistenceIDs))
	next := ""
	if end < len(x.persistenceIDs) {
		next = x.persistenceIDs[end-1]
	}

	return x.persistenceIDs[start:end], next, nil
}

// entityIDs returns count persistence IDs, sorted.
func entityIDs(count int) []string {
	persistenceIDs := make([]string, 0, count)
	for index := range count {
		persistenceIDs = append(persistenceIDs, fmt.Sprintf("entity-%03d", index))
	}
	return persistenceIDs
}

func TestForEachEntity(t *testing.T) {
	ctx := context.Background()

	t.Run("migrates every entity of every page, at most parallelism at a time", func(t *testing.T) {
		s := defaultSettings()
		s.pageSize = 3
		s.parallelism = 4

		var (
			mutex    sync.Mutex
			migrated []string
			inFlight atomic.Int32
			peak     atomic.Int32
		)

		total, err := forEachEntity(ctx, pagedLister{persistenceIDs: entityIDs(20)}, s, func(_ context.Context, persistenceID string) error {
			current := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				observed := peak.Load()
				if current <= observed || peak.CompareAndSwap(observed, current) {
					break
				}
			}

			mutex.Lock()
			migrated = append(migrated, persistenceID)
			mutex.Unlock()
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 20, total)
		assert.ElementsMatch(t, entityIDs(20), migrated)
		assert.LessOrEqual(t, peak.Load(), int32(4))
	})

	t.Run("stops at the first failing entity", func(t *testing.T) {
		s := defaultSettings()
		s.pageSize = 1
		s.parallelism = 1

		total, err := forEachEntity(ctx, pagedLister{persistenceIDs: entityIDs(5)}, s, func(_ context.Context, persistenceID string) error {
			if persistenceID == "entity-002" {
				return assert.AnError
			}
			return nil
		})

		require.ErrorIs(t, err, assert.AnError)
		assert.Equal(t, 2, total)
	})

	t.Run("reports a listing that fails", func(t *testing.T) {
		s := defaultSettings()
		s.pageSize = 2

		total, err := forEachEntity(ctx, pagedLister{persistenceIDs: entityIDs(5), failAt: "entity-001"}, s, func(context.Context, string) error {
			return nil
		})

		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "failed to list persistence IDs")
		assert.Equal(t, 2, total)
	})

	t.Run("reports the failing entity rather than the listing it cancelled", func(t *testing.T) {
		s := defaultSettings()
		s.pageSize = 1
		s.parallelism = 1
		entityErr := fmt.Errorf("entity failed")

		_, err := forEachEntity(ctx, pagedLister{persistenceIDs: entityIDs(3), failAt: "entity-000"}, s, func(context.Context, string) error {
			return entityErr
		})

		require.ErrorIs(t, err, entityErr)
	})

	t.Run("reports a cancelled migration as incomplete", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		total, err := forEachEntity(cancelled, pagedLister{persistenceIDs: entityIDs(3)}, defaultSettings(), func(context.Context, string) error {
			return nil
		})

		require.ErrorIs(t, err, context.Canceled)
		assert.Zero(t, total)
	})
}

func TestWithParallelism(t *testing.T) {
	s := defaultSettings()
	assert.Equal(t, 10, s.parallelism)

	WithParallelism(4).apply(&s)
	assert.Equal(t, 4, s.parallelism)

	WithParallelism(0).apply(&s)
	assert.Equal(t, 1, s.parallelism, "a parallelism below one is treated as one")
}

func TestWithPageSize(t *testing.T) {
	s := defaultSettings()
	assert.EqualValues(t, DefaultPageSize, s.pageSize)

	WithPageSize(4).apply(&s)
	assert.EqualValues(t, 4, s.pageSize)

	WithPageSize(0).apply(&s)
	assert.EqualValues(t, 1, s.pageSize, "a page size below one is treated as one")
}
