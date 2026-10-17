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
	"time"

	"github.com/tochemey/goakt/v4/log"

	"github.com/tochemey/ego/v5/encryption"
	"github.com/tochemey/ego/v5/eventadapter"
	"github.com/tochemey/ego/v5/eventstream"
	"github.com/tochemey/ego/v5/projection"
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

// withPullInterval sets how often the runner reads the journal. A value of
// zero or less keeps the default: the pull ticker needs a positive interval.
func withPullInterval(interval time.Duration) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		if interval <= 0 {
			return
		}

		runner.pullInterval = interval
	})
}

// withMaxBufferSize sets how many events the runner reads from a shard, and
// replays of one entity, at once. A value below one keeps the default: a buffer
// of zero events could never make progress.
func withMaxBufferSize(bufferSize int) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		if bufferSize < 1 {
			return
		}

		runner.maxBufferSize = bufferSize
	})
}

// withTransactionalHandler makes the runner hand each event to the given
// transactional handler, which records the event's offset in its own
// transaction, instead of to its handler.
func withTransactionalHandler(handler projection.TransactionalHandler) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.transactionalHandler = handler
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

// withDeadLetterHandler sets the dead letter handler for the projection runner
func withDeadLetterHandler(handler projection.DeadLetterHandler) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.deadLetterHandler = handler
	})
}

// withEventAdapters sets the event adapters for the projection runner
func withEventAdapters(adapters []eventadapter.EventAdapter) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.eventAdapters = adapters
	})
}

// withMetrics sets the metrics for the projection runner
func withMetrics(m *metrics) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.metrics = m
	})
}

// withEncryptor sets the encryptor for the projection runner
func withEncryptor(enc encryption.Encryptor) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.encryptor = enc
	})
}

// withEventsStream sets the in-process events stream that triggers an
// immediate pull when events are persisted on the local node.
func withEventsStream(stream eventstream.Stream) runnerOption {
	return runnerOptionFunc(func(runner *projectionRunner) {
		runner.eventsStream = stream
	})
}
