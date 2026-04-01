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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryWithBackoff(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		var calls int32
		err := retryWithBackoff(context.Background(), defaultMaxRetries, func() error {
			atomic.AddInt32(&calls, 1)
			return nil
		})
		require.NoError(t, err)
		assert.EqualValues(t, 1, atomic.LoadInt32(&calls))
	})
	t.Run("succeeds on second attempt", func(t *testing.T) {
		var calls int32
		err := retryWithBackoff(context.Background(), defaultMaxRetries, func() error {
			n := atomic.AddInt32(&calls, 1)
			if n < 2 {
				return errors.New("transient")
			}
			return nil
		})
		require.NoError(t, err)
		assert.EqualValues(t, 2, atomic.LoadInt32(&calls))
	})
	t.Run("succeeds on last attempt", func(t *testing.T) {
		maxRetries := 3
		var calls int32
		err := retryWithBackoff(context.Background(), maxRetries, func() error {
			n := atomic.AddInt32(&calls, 1)
			if int(n) <= maxRetries {
				return errors.New("transient")
			}
			return nil
		})
		require.NoError(t, err)
		assert.EqualValues(t, maxRetries+1, atomic.LoadInt32(&calls))
	})
	t.Run("returns last error after all attempts exhausted", func(t *testing.T) {
		sentinel := errors.New("persistent failure")
		var calls int32
		err := retryWithBackoff(context.Background(), defaultMaxRetries, func() error {
			atomic.AddInt32(&calls, 1)
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)
		assert.EqualValues(t, defaultMaxRetries+1, atomic.LoadInt32(&calls))
	})
	t.Run("zero max retries executes exactly once", func(t *testing.T) {
		var calls int32
		sentinel := errors.New("fail")
		err := retryWithBackoff(context.Background(), 0, func() error {
			atomic.AddInt32(&calls, 1)
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)
		assert.EqualValues(t, 1, atomic.LoadInt32(&calls))
	})
	t.Run("context cancelled before retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var calls int32
		err := retryWithBackoff(ctx, defaultMaxRetries, func() error {
			n := atomic.AddInt32(&calls, 1)
			if n == 1 {
				cancel()
			}
			return errors.New("transient")
		})
		require.ErrorIs(t, err, context.Canceled)
		assert.EqualValues(t, 1, atomic.LoadInt32(&calls))
	})
	t.Run("context deadline exceeded before retry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		var calls int32
		err := retryWithBackoff(ctx, 10, func() error {
			atomic.AddInt32(&calls, 1)
			return errors.New("transient")
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		got := atomic.LoadInt32(&calls)
		assert.True(t, got >= 1 && got < 10, "expected partial attempts, got %d", got)
	})
	t.Run("backoff delays increase between attempts", func(t *testing.T) {
		timestamps := make([]time.Time, 0, 4)
		err := retryWithBackoff(context.Background(), 2, func() error {
			timestamps = append(timestamps, time.Now())
			return errors.New("fail")
		})
		require.Error(t, err)
		require.Len(t, timestamps, 3)

		delay1 := timestamps[1].Sub(timestamps[0])
		delay2 := timestamps[2].Sub(timestamps[1])

		assert.True(t, delay1 >= 50*time.Millisecond, "first delay %v too short", delay1)
		assert.True(t, delay2 > delay1, "second delay %v should exceed first %v", delay2, delay1)
	})
}
