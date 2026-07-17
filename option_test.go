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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/projection"
	"github.com/tochemey/ego/v4/testkit"
)

// buildActorSystem constructs and starts a goakt actor system from a Config so
// the optional extension branches in Config.GoaktOptions can be inspected.
func buildActorSystem(t *testing.T, cfg *Config) goakt.ActorSystem {
	t.Helper()
	ctx := context.Background()
	sys, err := goakt.NewActorSystem("OptionTest", cfg.GoaktOptions()...)
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))
	t.Cleanup(func() { _ = sys.Stop(ctx) })
	return sys
}

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
	c := NewConfig(nil, WithProjection("accounts", o))
	require.NotNil(t, c.projections)
	assert.Same(t, o, c.projections["accounts"])
}

func TestOptionWithProjectionMultiple(t *testing.T) {
	accounts := &projection.Options{Handler: projection.NewDiscardHandler()}
	audit := &projection.Options{Handler: projection.NewDiscardHandler()}
	c := NewConfig(nil,
		WithProjection("accounts", accounts),
		WithProjection("audit", audit),
	)
	require.Len(t, c.projections, 2)
	assert.Same(t, accounts, c.projections["accounts"])
	assert.Same(t, audit, c.projections["audit"])
}

func TestOptionWithProjectionNil(t *testing.T) {
	c := NewConfig(nil, WithProjection("accounts", nil))
	assert.Nil(t, c.projections)
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

func TestOptionWithLoggerNilFallback(t *testing.T) {
	// Passing a nil Logger via WithLogger should round-trip through
	// NewConfig and land on the default logger (isNilLogger fallback).
	c := NewConfig(nil, WithLogger(nil))
	require.NotNil(t, c.logger)
	_, ok := c.logger.(defaultLogger)
	assert.True(t, ok, "expected defaultLogger fallback, got %T", c.logger)
}

func TestConfigGoaktOptionsEncryptor(t *testing.T) {
	// WithEncryptor must register the Encryptor extension via GoaktOptions.
	enc := encryption.NewAESEncryptor(testkit.NewKeyStore())
	cfg := NewConfig(testkit.NewEventsStore(), WithEncryptor(enc))

	sys := buildActorSystem(t, cfg)
	require.NotNil(t, sys.Extension(extensions.EncryptorExtensionID))
}

func TestConfigGoaktOptionsTelemetry(t *testing.T) {
	// WithTelemetry must register the Telemetry extension via GoaktOptions.
	tel := &Telemetry{
		Tracer: nooptrace.NewTracerProvider().Tracer("test"),
		Meter:  noopmetric.NewMeterProvider().Meter("test"),
	}
	cfg := NewConfig(testkit.NewEventsStore(), WithTelemetry(tel))

	sys := buildActorSystem(t, cfg)
	require.NotNil(t, sys.Extension(extensions.TelemetryExtensionID))
}

func TestConfigGoaktOptionsProjectionDefaultsRecovery(t *testing.T) {
	// When a projection is configured without a Recovery, GoaktOptions
	// should still register the extension by falling back to a default
	// recovery strategy.
	cfg := NewConfig(testkit.NewEventsStore(),
		WithProjection("accounts", &projection.Options{
			Handler:      projection.NewDiscardHandler(),
			BufferSize:   10,
			PullInterval: time.Second,
		}),
	)

	sys := buildActorSystem(t, cfg)
	require.NotNil(t, sys.Extension(extensions.ProjectionExtensionID))
}

func TestClusterKindsExposesEgoActors(t *testing.T) {
	kinds := ClusterKinds()
	require.Len(t, kinds, 4)
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
