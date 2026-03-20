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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/projection"
	"github.com/tochemey/ego/v4/testkit"
)

func TestOption(t *testing.T) {
	ts := time.Second
	from := time.Now()
	to := time.Now().Add(ts)
	recovery := projection.NewRecovery()

	t.Run("WithRefreshInterval", func(t *testing.T) {
		var r projectionRunner
		withPullInterval(ts).Apply(&r)
		assert.Equal(t, ts, r.pullInterval)
	})
	t.Run("WithMaxBufferSize", func(t *testing.T) {
		var r projectionRunner
		withMaxBufferSize(5).Apply(&r)
		assert.Equal(t, 5, r.maxBufferSize)
	})
	t.Run("WithStartOffset", func(t *testing.T) {
		var r projectionRunner
		withStartOffset(from).Apply(&r)
		assert.Equal(t, from, r.startingOffset)
	})
	t.Run("WithResetOffset", func(t *testing.T) {
		var r projectionRunner
		withResetOffset(to).Apply(&r)
		assert.Equal(t, to, r.resetOffsetTo)
	})
	t.Run("WithLogger", func(t *testing.T) {
		var r projectionRunner
		withLogger(log.DefaultLogger).Apply(&r)
		assert.Equal(t, log.DefaultLogger, r.logger)
	})
	t.Run("WithRecoveryStrategy", func(t *testing.T) {
		var r projectionRunner
		withRecoveryStrategy(recovery).Apply(&r)
		assert.Equal(t, recovery, r.recovery)
	})
}

func TestWithDeadLetterHandler(t *testing.T) {
	dlh := projection.NewDiscardDeadLetterHandler()
	var r projectionRunner
	withDeadLetterHandler(dlh).Apply(&r)
	require.NotNil(t, r.deadLetterHandler)
	assert.Equal(t, dlh, r.deadLetterHandler)
}

func TestWithDeadLetterHandlerNil(t *testing.T) {
	var r projectionRunner
	withDeadLetterHandler(nil).Apply(&r)
	assert.Nil(t, r.deadLetterHandler)
}

func TestWithEventAdapters(t *testing.T) {
	adapter := &runnerTestAdapter{}
	adapters := []eventadapter.EventAdapter{adapter}
	var r projectionRunner
	withEventAdapters(adapters).Apply(&r)
	require.Len(t, r.eventAdapters, 1)
	assert.Equal(t, adapter, r.eventAdapters[0])
}

func TestWithEventAdaptersEmpty(t *testing.T) {
	var r projectionRunner
	withEventAdapters(nil).Apply(&r)
	assert.Nil(t, r.eventAdapters)
}

func TestWithMetrics(t *testing.T) {
	m := &metrics{}
	var r projectionRunner
	withMetrics(m).Apply(&r)
	assert.Equal(t, m, r.metrics)
}

func TestWithEncryptor(t *testing.T) {
	enc := encryption.NewAESEncryptor(testkit.NewKeyStore())
	var r projectionRunner
	withEncryptor(enc).Apply(&r)
	require.NotNil(t, r.encryptor)
	assert.Equal(t, enc, r.encryptor)
}

// runnerTestAdapter is a no-op EventAdapter for testing
type runnerTestAdapter struct{}

func (a *runnerTestAdapter) Adapt(event *anypb.Any, _ uint64) (*anypb.Any, error) {
	return event, nil
}
