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

package testkit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/ego/v4/egopb"
)

// TestOffsetStoreResetOffset asserts that a reset rewrites every shard of the
// named projection and leaves other projections alone.
func TestOffsetStoreResetOffset(t *testing.T) {
	ctx := context.Background()
	store := NewOffsetStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	require.NoError(t, store.WriteOffset(ctx, &egopb.Offset{ProjectionName: "balances", ShardNumber: 1, Value: 10}))
	require.NoError(t, store.WriteOffset(ctx, &egopb.Offset{ProjectionName: "balances", ShardNumber: 2, Value: 20}))
	require.NoError(t, store.WriteOffset(ctx, &egopb.Offset{ProjectionName: "audit", ShardNumber: 1, Value: 30}))

	require.NoError(t, store.ResetOffset(ctx, "balances", 5))

	for _, shard := range []uint64{1, 2} {
		offset, err := store.GetCurrentOffset(ctx, &egopb.ProjectionId{ProjectionName: "balances", ShardNumber: shard})
		require.NoError(t, err)
		assert.EqualValues(t, 5, offset.GetValue())
	}

	offset, err := store.GetCurrentOffset(ctx, &egopb.ProjectionId{ProjectionName: "audit", ShardNumber: 1})
	require.NoError(t, err)
	assert.EqualValues(t, 30, offset.GetValue())
}
