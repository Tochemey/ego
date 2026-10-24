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

package persistence

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardFor(t *testing.T) {
	t.Run("returns the frozen shard of a persistence ID", func(t *testing.T) {
		// These values are part of the journal layout: a change here moves
		// persistence IDs to other shards and breaks every existing journal.
		golden := map[string]uint64{
			"":          805,
			"account-1": 598,
			"saga-42":   582,
		}

		for persistenceID, shard := range golden {
			assert.Equalf(t, shard, ShardFor(persistenceID), "shard of %q", persistenceID)
		}
	})

	t.Run("stays within the shard range", func(t *testing.T) {
		for _, persistenceID := range []string{"a", "account-1", "order-9f2c", "a-very-long-persistence-id-of-an-entity"} {
			assert.Less(t, ShardFor(persistenceID), NumberOfShards)
		}
	})
}
