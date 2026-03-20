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

	goset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/discovery/kubernetes"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/projection"
	"github.com/tochemey/ego/v4/testkit"
)

func TestOptions(t *testing.T) {
	// use the discard logger
	logger := DiscardLogger
	// create a discovery provider
	discoveryProvider := kubernetes.NewDiscovery(&kubernetes.Config{})

	testCases := []struct {
		name     string
		option   Option
		expected func() *Engine
	}{
		{
			name:   "WithCluster",
			option: WithCluster(discoveryProvider, 30, 3, "localhost", 1334, 1335, 1336),
			expected: func() *Engine {
				expected := &Engine{
					discoveryProvider:  discoveryProvider,
					minimumPeersQuorum: 3,
					bindAddr:           "localhost",
					discoveryPort:      1335,
					peersPort:          1336,
					remotingPort:       1334,
					partitionsCount:    30,
				}
				expected.clusterEnabled.Store(true)
				return expected
			},
		},
		{
			name:   "WithLogger",
			option: WithLogger(logger),
			expected: func() *Engine {
				return &Engine{logger: logger}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			engine := new(Engine)
			tc.option.Apply(engine)
			assert.Equal(t, tc.expected(), engine)
		})
	}
}

func TestOptionWithProjection(t *testing.T) {
	handler := projection.NewDiscardHandler()
	recovery := projection.NewRecovery(projection.WithRetries(10))
	engine := new(Engine)
	opt := WithProjection(&projection.Options{
		Handler:      handler,
		BufferSize:   500,
		StartOffset:  time.Time{},
		ResetOffset:  time.Time{},
		PullInterval: time.Second,
		Recovery:     recovery,
	})
	opt.Apply(engine)
	require.NotEmpty(t, engine.projectionExtension)
	require.Same(t, recovery, engine.projectionExtension.Recovery())
}

func TestOptionWithRoles(t *testing.T) {
	engine := &Engine{roles: goset.NewSet[string]()}
	opt := WithRoles("role1", "role2")
	opt.Apply(engine)
	assert.ElementsMatch(t, []string{"role1", "role2"}, engine.roles.ToSlice())
}

func TestOptionWithTelemetry(t *testing.T) {
	tracer := nooptrace.NewTracerProvider().Tracer("test")
	tel := &Telemetry{Tracer: tracer}
	engine := new(Engine)
	opt := WithTelemetry(tel)
	opt.Apply(engine)
	require.NotNil(t, engine.telemetry)
	assert.Equal(t, tracer, engine.telemetry.Tracer)
}

func TestOptionWithTelemetryNil(t *testing.T) {
	engine := new(Engine)
	opt := WithTelemetry(nil)
	opt.Apply(engine)
	assert.Nil(t, engine.telemetry)
}

func TestOptionWithEventAdapters(t *testing.T) {
	adapter := &testEventAdapter{}
	engine := new(Engine)
	opt := WithEventAdapters(adapter)
	opt.Apply(engine)
	require.Len(t, engine.eventAdapters, 1)
	assert.Equal(t, adapter, engine.eventAdapters[0])
}

func TestOptionWithEventAdaptersMultiple(t *testing.T) {
	a1 := &testEventAdapter{}
	a2 := &testEventAdapter{}
	engine := new(Engine)
	WithEventAdapters(a1).Apply(engine)
	WithEventAdapters(a2).Apply(engine)
	require.Len(t, engine.eventAdapters, 2)
}

func TestOptionWithProjectionDeadLetterHandler(t *testing.T) {
	handler := projection.NewDiscardHandler()
	dlh := projection.NewDiscardDeadLetterHandler()
	engine := new(Engine)
	opt := WithProjection(&projection.Options{
		Handler:           handler,
		BufferSize:        100,
		PullInterval:      time.Second,
		DeadLetterHandler: dlh,
	})
	opt.Apply(engine)
	require.NotNil(t, engine.projectionExtension)
	assert.Equal(t, dlh, engine.projectionExtension.DeadLetterHandler())
}

func TestOptionWithProjectionNilRecoveryDefaultsToNewRecovery(t *testing.T) {
	handler := projection.NewDiscardHandler()
	engine := new(Engine)
	opt := WithProjection(&projection.Options{
		Handler:      handler,
		PullInterval: time.Second,
		Recovery:     nil,
	})
	opt.Apply(engine)
	require.NotNil(t, engine.projectionExtension)
	assert.NotNil(t, engine.projectionExtension.Recovery())
}

func TestOptionWithProjectionNil(t *testing.T) {
	engine := new(Engine)
	opt := WithProjection(nil)
	opt.Apply(engine)
	assert.Nil(t, engine.projectionExtension)
}

func TestOptionWithSnapshotStore(t *testing.T) {
	store := testkit.NewSnapshotStore()
	engine := new(Engine)
	opt := WithSnapshotStore(store)
	opt.Apply(engine)
	require.NotNil(t, engine.snapshotStore)
	assert.Equal(t, store, engine.snapshotStore)
}

func TestOptionWithEncryptor(t *testing.T) {
	enc := encryption.NewAESEncryptor(testkit.NewKeyStore())
	engine := new(Engine)
	opt := WithEncryptor(enc)
	opt.Apply(engine)
	require.NotNil(t, engine.encryptor)
	assert.Equal(t, enc, engine.encryptor)
}

func TestOptionWithStateStore(t *testing.T) {
	store := testkit.NewDurableStore()
	engine := new(Engine)
	opt := WithStateStore(store)
	opt.Apply(engine)
	require.NotNil(t, engine.stateStore)
	assert.Equal(t, store, engine.stateStore)
}

func TestOptionWithOffsetStore(t *testing.T) {
	store := testkit.NewOffsetStore()
	engine := new(Engine)
	opt := WithOffsetStore(store)
	opt.Apply(engine)
	require.NotNil(t, engine.offsetStore)
	assert.Equal(t, store, engine.offsetStore)
}

func TestOptionWithTLS(t *testing.T) {
	tls := &TLS{}
	engine := new(Engine)
	opt := WithTLS(tls)
	opt.Apply(engine)
	require.NotNil(t, engine.tls)
	assert.Equal(t, tls, engine.tls)
}

// testEventAdapter is a no-op EventAdapter for testing
type testEventAdapter struct{}

var _ eventadapter.EventAdapter = (*testEventAdapter)(nil)

func (a *testEventAdapter) Adapt(event *anypb.Any, _ uint64) (*anypb.Any, error) {
	return event, nil
}

// ensure trace.Tracer is used to satisfy the compiler
var _ trace.Tracer = nooptrace.NewTracerProvider().Tracer("compile-check")
