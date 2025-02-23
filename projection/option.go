/*
 * MIT License
 *
 * Copyright (c) 2023-2025 Tochemey
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

package projection

import (
	"time"

	"github.com/tochemey/goakt/v3/log"
)

// Option is the interface that applies a configuration option.
type Option interface {
	// Apply sets the Option value of a config.
	Apply(runner *runner)
}

var _ Option = OptionFunc(nil)

// OptionFunc implements the Option interface.
type OptionFunc func(*runner)

// Apply applies the options to Engine
func (f OptionFunc) Apply(runner *runner) {
	f(runner)
}

// WithPullInterval sets the events pull interval
// This defines how often the projection will fetch events
func WithPullInterval(interval time.Duration) Option {
	return OptionFunc(func(runner *runner) {
		runner.pullInterval = interval
	})
}

// WithMaxBufferSize sets the max buffer size.
// This defines how many events are fetched on a single run of the projection
func WithMaxBufferSize(bufferSize int) Option {
	return OptionFunc(func(runner *runner) {
		runner.maxBufferSize = bufferSize
	})
}

// WithStartOffset sets the starting point where to read the events
func WithStartOffset(startOffset time.Time) Option {
	return OptionFunc(func(runner *runner) {
		runner.startingOffset = startOffset
	})
}

// WithResetOffset helps reset the offset to a given timestamp.
func WithResetOffset(resetOffset time.Time) Option {
	return OptionFunc(func(runner *runner) {
		runner.resetOffsetTo = resetOffset
	})
}

// WithLogger sets the actor system custom log
func WithLogger(logger log.Logger) Option {
	return OptionFunc(func(runner *runner) {
		runner.logger = logger
	})
}

// WithRecoveryStrategy sets the recovery strategy
func WithRecoveryStrategy(strategy *Recovery) Option {
	return OptionFunc(func(runner *runner) {
		runner.recovery = strategy
	})
}
