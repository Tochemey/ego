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
	"math"
	"math/rand/v2"
	"time"
)

const (
	defaultMaxRetries = 3
	retryBaseDelay    = 100 * time.Millisecond
	retryMaxDelay     = 2 * time.Second
)

// retryWithBackoff retries op up to maxRetries times with exponential backoff
// and jitter. Returns nil on the first successful attempt or the last error
// after all attempts are exhausted. Respects context cancellation between
// attempts.
func retryWithBackoff(ctx context.Context, maxRetries int, op func() error) error {
	var err error
	for attempt := range maxRetries + 1 {
		if err = op(); err == nil {
			return nil
		}

		if attempt < maxRetries {
			delay := time.Duration(math.Min(
				float64(retryBaseDelay)*math.Pow(2, float64(attempt)),
				float64(retryMaxDelay),
			))
			jitter := 0.5 + rand.Float64() //nolint:gosec // cryptographic randomness is not needed for backoff jitter
			delay = time.Duration(float64(delay) * jitter)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return err
}
