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
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/projection"
	"github.com/tochemey/ego/v4/testkit"
)

func TestOptionWithLogger(t *testing.T) {
	c := NewConfig(nil, WithLogger(DiscardLogger))
	assert.Equal(t, DiscardLogger, c.logger)
}

func TestOptionWithStateStore(t *testing.T) {
	store := testkit.NewDurableStore()
	c := NewConfig(nil, WithStateStore(store))
	assert.Equal(t, store, c.stateStore)
}

func TestOptionWithOffsetStore(t *testing.T) {
	store := testkit.NewOffsetStore()
	c := NewConfig(nil, WithOffsetStore(store))
	assert.Equal(t, store, c.offsetStore)
}

func TestOptionWithSnapshotStore(t *testing.T) {
	store := testkit.NewSnapshotStore()
	c := NewConfig(nil, WithSnapshotStore(store))
	assert.Equal(t, store, c.snapshotStore)
}

func TestOptionWithEncryptor(t *testing.T) {
	enc := encryption.NewAESEncryptor(testkit.NewKeyStore())
	c := NewConfig(nil, WithEncryptor(enc))
	assert.Equal(t, enc, c.encryptor)
}

func TestOptionWithProjection(t *testing.T) {
	handler := projection.NewDiscardHandler()
	recovery := projection.NewRecovery(projection.WithRetries(10))
	o := &projection.Options{
		Handler:      handler,
		BufferSize:   500,
		PullInterval: time.Second,
		Recovery:     recovery,
	}
	c := NewConfig(nil, WithProjection(o))
	require.NotNil(t, c.projection)
	assert.Same(t, o, c.projection)
}

func TestOptionWithProjectionNil(t *testing.T) {
	c := NewConfig(nil, WithProjection(nil))
	assert.Nil(t, c.projection)
}

func TestOptionWithTelemetry(t *testing.T) {
	tracer := nooptrace.NewTracerProvider().Tracer("test")
	tel := &Telemetry{Tracer: tracer}
	c := NewConfig(nil, WithTelemetry(tel))
	require.NotNil(t, c.telemetry)
	assert.Equal(t, tracer, c.telemetry.Tracer)
}

func TestOptionWithTelemetryNil(t *testing.T) {
	c := NewConfig(nil, WithTelemetry(nil))
	assert.Nil(t, c.telemetry)
}

func TestOptionWithEventAdapters(t *testing.T) {
	adapter := &testEventAdapter{}
	c := NewConfig(nil, WithEventAdapters(adapter))
	require.Len(t, c.eventAdapters, 1)
	assert.Equal(t, adapter, c.eventAdapters[0])
}

func TestOptionWithEventAdaptersMultiple(t *testing.T) {
	c := NewConfig(nil,
		WithEventAdapters(&testEventAdapter{}),
		WithEventAdapters(&testEventAdapter{}),
	)
	assert.Len(t, c.eventAdapters, 2)
}

// testEventAdapter is a no-op EventAdapter for testing.
type testEventAdapter struct{}

var _ eventadapter.EventAdapter = (*testEventAdapter)(nil)

func (a *testEventAdapter) Adapt(event *anypb.Any, _ uint64) (*anypb.Any, error) {
	return event, nil
}

// keep the trace and nooptrace packages referenced even when no test uses
// them directly.
var _ trace.Tracer = nooptrace.NewTracerProvider().Tracer("compile-check")
