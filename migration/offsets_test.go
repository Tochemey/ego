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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocksoffsetstore "github.com/tochemey/ego/v5/mocks/offsetstore"
	"github.com/tochemey/ego/v5/testkit"
)

// legacyOffsets is a LegacyOffsetReader over fixed offsets.
type legacyOffsets struct {
	// offsets holds the legacy offsets of every projection.
	offsets map[string][]int64
	// err is returned instead of the offsets when set.
	err error
}

// LegacyOffsets returns the fixed offsets, or the configured error.
func (x legacyOffsets) LegacyOffsets(context.Context) (map[string][]int64, error) {
	return x.offsets, x.err
}

func TestResumeProjection(t *testing.T) {
	ctx := context.Background()

	t.Run("starts from the oldest legacy offset", func(t *testing.T) {
		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		require.NoError(t, ResumeProjection(ctx, offsetStore, "balances", []int64{300, 100, 200}))

		start, err := offsetStore.StartOffset(ctx, "balances")
		require.NoError(t, err)
		assert.EqualValues(t, 100, start)
	})

	t.Run("starts from the beginning without legacy offsets", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ResetOffset(mock.Anything, "balances", int64(0)).Return(nil)

		require.NoError(t, ResumeProjection(ctx, offsetStore, "balances", nil))
		offsetStore.AssertExpectations(t)
	})

	t.Run("fails when the start cannot be recorded", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ResetOffset(mock.Anything, "balances", int64(100)).Return(assert.AnError)

		err := ResumeProjection(ctx, offsetStore, "balances", []int64{100})
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "failed to resume projection balances")
	})
}

func TestResumeProjections(t *testing.T) {
	ctx := context.Background()

	t.Run("resumes every projection from its oldest legacy offset", func(t *testing.T) {
		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		resumed, err := ResumeProjections(ctx, offsetStore, legacyOffsets{offsets: map[string][]int64{
			"balances": {300, 100},
			"audit":    {50},
		}})
		require.NoError(t, err)
		assert.Equal(t, 2, resumed)

		start, err := offsetStore.StartOffset(ctx, "balances")
		require.NoError(t, err)
		assert.EqualValues(t, 100, start)

		start, err = offsetStore.StartOffset(ctx, "audit")
		require.NoError(t, err)
		assert.EqualValues(t, 50, start)
	})

	t.Run("fails when the legacy offsets cannot be read", func(t *testing.T) {
		_, err := ResumeProjections(ctx, new(mocksoffsetstore.OffsetStore), legacyOffsets{err: assert.AnError})
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "failed to read the legacy offsets")
	})

	t.Run("stops at the first projection that cannot resume, in name order", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ResetOffset(mock.Anything, "audit", int64(50)).Return(nil)
		offsetStore.EXPECT().ResetOffset(mock.Anything, "balances", int64(100)).Return(assert.AnError)

		resumed, err := ResumeProjections(ctx, offsetStore, legacyOffsets{offsets: map[string][]int64{
			"balances": {100},
			"audit":    {50},
			"zeta":     {10},
		}})
		require.ErrorIs(t, err, assert.AnError)
		assert.Equal(t, 1, resumed)
		offsetStore.AssertNotCalled(t, "ResetOffset", mock.Anything, "zeta", mock.Anything)
	})
}
