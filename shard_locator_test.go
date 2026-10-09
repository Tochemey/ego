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

package ego

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
)

func TestShardLocator(t *testing.T) {
	t.Run("looks the shard up once and keeps it", func(t *testing.T) {
		ctx := context.Background()
		actorSystem, err := goakt.NewActorSystem("TestSystem", goakt.WithLogger(log.DiscardLogger))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		t.Cleanup(func() { _ = actorSystem.Stop(ctx) })

		locator := shardLocator{}
		// outside a cluster every actor is on partition 0
		assert.EqualValues(t, 0, locator.locate(actorSystem, "entity-1"))
		assert.True(t, locator.resolved)

		// a resolved locator never consults the actor system again: a lookup
		// through a nil system would panic
		assert.EqualValues(t, 0, locator.locate(nil, "entity-1"))
	})

	t.Run("returns a shard set in advance without a lookup", func(t *testing.T) {
		locator := shardLocator{resolved: true, shard: 7}
		assert.EqualValues(t, 7, locator.locate(nil, "entity-1"))
	})
}
