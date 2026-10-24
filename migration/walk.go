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
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// persistenceIDLister lists the persistence IDs of a journal a page at a time.
type persistenceIDLister interface {
	PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error)
}

// forEachEntity runs migrate for every persistence ID of the journal and
// returns how many succeeded.
//
// The persistence IDs are read one page at a time and migrated by at most
// parallelism workers, so memory holds one page and the entities in flight,
// whatever the size of the journal. The first failure cancels the context of
// the other workers, stops the listing and is returned.
func forEachEntity(ctx context.Context, lister persistenceIDLister, s settings, migrate func(ctx context.Context, persistenceID string) error) (int, error) {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(s.parallelism)

	var migrated atomic.Int64
	pageToken := ""

	for groupCtx.Err() == nil {
		persistenceIDs, nextToken, err := lister.PersistenceIDs(groupCtx, s.pageSize, pageToken)
		if err != nil {
			if waitErr := group.Wait(); waitErr != nil {
				return int(migrated.Load()), waitErr
			}

			return int(migrated.Load()), fmt.Errorf("failed to list persistence IDs: %w", err)
		}

		for _, persistenceID := range persistenceIDs {
			group.Go(func() error {
				// an entity queued behind the parallelism limit does not start
				// once another one failed
				if groupCtx.Err() != nil {
					return nil
				}

				if err := migrate(groupCtx, persistenceID); err != nil {
					return err
				}

				migrated.Add(1)
				return nil
			})
		}

		if nextToken == "" || len(persistenceIDs) == 0 {
			break
		}
		pageToken = nextToken
	}

	if err := group.Wait(); err != nil {
		return int(migrated.Load()), err
	}

	// a cancelled migration stopped early: it did not complete
	return int(migrated.Load()), ctx.Err()
}
