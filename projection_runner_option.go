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
	"time"

	"github.com/tochemey/goakt/v3/log"

	"github.com/tochemey/ego/v3/projection"
)

// Option is the interface that applies a configuration option.
type runnerOption interface {
	// Apply sets the Option value of a config.
	Apply(runner *projectionRunner)
}

var _ runnerOption = runnerOptionFunc(nil)

// OptionFunc implements the Option interface.
type runnerOptionFunc func(*projectionRunner)

// Apply applies the options to Engine
func (f runnerOptionFunc) Apply(runner *projectionRunner) {
	f(runner)
}

// WithPullInterval sets the events pull interval
// This defines how often the projection will fetch events
func withPullInterval(interval time.Duration) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.pullInterval = interval
	})
}

// withMaxBufferSize sets the max buffer size.
// This defines how many events are fetched on a single run of the projection
func withMaxBufferSize(bufferSize int) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.maxBufferSize = bufferSize
	})
}

// withStartOffset sets the starting point where to read the events
func withStartOffset(startOffset time.Time) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.startingOffset = startOffset
	})
}

// withResetOffset helps reset the offset to a given timestamp.
func withResetOffset(resetOffset time.Time) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.resetOffsetTo = resetOffset
	})
}

// WithLogger sets the actor system custom log
func withLogger(logger log.Logger) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.logger = logger
	})
}

// withRecoveryStrategy sets the recovery strategy
func withRecoveryStrategy(strategy *projection.Recovery) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.recovery = strategy
	})
}
