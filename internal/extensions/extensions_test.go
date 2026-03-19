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

package extensions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/projection"
	"github.com/tochemey/ego/v4/testkit"
)

func TestEventsStore(t *testing.T) {
	store := testkit.NewEventsStore()
	ext := NewEventsStore(store)

	require.NotNil(t, ext)
	assert.Equal(t, EventsStoreExtensionID, ext.ID())
	assert.Equal(t, store, ext.Underlying())
}

func TestDurableStateStore(t *testing.T) {
	store := testkit.NewDurableStore()
	ext := NewDurableStateStore(store)

	require.NotNil(t, ext)
	assert.Equal(t, DurableStateStoreExtensionID, ext.ID())
	assert.Equal(t, store, ext.Underlying())
}

func TestEventsStream(t *testing.T) {
	stream := eventstream.New()
	ext := NewEventsStream(stream)

	require.NotNil(t, ext)
	assert.Equal(t, EventsStreamExtensionID, ext.ID())
	assert.Equal(t, stream, ext.Underlying())
}

func TestOffsetStore(t *testing.T) {
	store := testkit.NewOffsetStore()
	ext := NewOffsetStore(store)

	require.NotNil(t, ext)
	assert.Equal(t, OffsetStoreExtensionID, ext.ID())
	assert.Equal(t, store, ext.Underlying())
}

func TestProjectionExtension(t *testing.T) {
	handler := projection.NewDiscardHandler()
	bufferSize := 100
	startOffset := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	resetOffset := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	pullInterval := 500 * time.Millisecond
	recovery := projection.NewRecovery()
	deadLetterHandler := projection.NewDiscardDeadLetterHandler()

	ext := NewProjectionExtension(
		handler,
		bufferSize,
		startOffset,
		resetOffset,
		pullInterval,
		recovery,
		deadLetterHandler,
	)

	require.NotNil(t, ext)
	assert.Equal(t, ProjectionExtensionID, ext.ID())
	assert.Equal(t, handler, ext.Handler())
	assert.Equal(t, bufferSize, ext.BufferSize())
	assert.Equal(t, startOffset, ext.StartOffset())
	assert.Equal(t, resetOffset, ext.ResetOffset())
	assert.Equal(t, recovery, ext.Recovery())
	assert.Equal(t, pullInterval, ext.PullInterval())
	assert.Equal(t, deadLetterHandler, ext.DeadLetterHandler())
}

func TestEventAdapters(t *testing.T) {
	t.Run("with nil adapters", func(t *testing.T) {
		ext := NewEventAdapters(nil)

		require.NotNil(t, ext)
		assert.Equal(t, EventAdaptersExtensionID, ext.ID())
		assert.Nil(t, ext.Adapters())
	})

	t.Run("with empty adapters", func(t *testing.T) {
		adapters := []eventadapter.EventAdapter{}
		ext := NewEventAdapters(adapters)

		require.NotNil(t, ext)
		assert.Equal(t, EventAdaptersExtensionID, ext.ID())
		assert.Empty(t, ext.Adapters())
	})
}

func TestTelemetryExtension(t *testing.T) {
	tracer := tracenoop.NewTracerProvider().Tracer("test")
	meter := noop.NewMeterProvider().Meter("test")

	ext := NewTelemetryExtension(tracer, meter)

	require.NotNil(t, ext)
	assert.Equal(t, TelemetryExtensionID, ext.ID())
	assert.Equal(t, tracer, ext.Tracer())
	assert.Equal(t, meter, ext.Meter())
}
