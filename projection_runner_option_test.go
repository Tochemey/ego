/*
 * MIT License
 *
 * Copyright (c) 2022-2025 Arsene Tochemey Gandote
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ego

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tochemey/goakt/v3/log"

	"github.com/tochemey/ego/v3/projection"
)

func TestOption(t *testing.T) {
	ts := time.Second
	from := time.Now()
	to := time.Now().Add(ts)
	recovery := projection.NewRecovery()
	testCases := []struct {
		name     string
		option   runnerOption
		expected projectionRunner
	}{
		{
			name:     "WithRefreshInterval",
			option:   withPullInterval(ts),
			expected: projectionRunner{pullInterval: ts},
		},
		{
			name:     "WithMaxBufferSize",
			option:   withMaxBufferSize(5),
			expected: projectionRunner{maxBufferSize: 5},
		},
		{
			name:     "WithStartOffset",
			option:   withStartOffset(from),
			expected: projectionRunner{startingOffset: from},
		},
		{
			name:     "WithResetOffset",
			option:   withResetOffset(to),
			expected: projectionRunner{resetOffsetTo: to},
		},
		{
			name:     "WithLogger",
			option:   withLogger(log.DefaultLogger),
			expected: projectionRunner{logger: log.DefaultLogger},
		},
		{
			name:     "WithRecoveryStrategy",
			option:   withRecoveryStrategy(recovery),
			expected: projectionRunner{recovery: recovery},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e projectionRunner
			tc.option.Apply(&e)
			assert.Equal(t, tc.expected, e)
		})
	}
}
