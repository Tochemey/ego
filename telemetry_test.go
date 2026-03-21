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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestNewMetrics_NilMeter(t *testing.T) {
	m := newMetrics(nil)
	assert.Nil(t, m)
}

func TestNewMetrics_WithMeter(t *testing.T) {
	meter := noopmetric.NewMeterProvider().Meter("test")
	m := newMetrics(meter)
	require.NotNil(t, m)
	assert.NotNil(t, m.commandsTotal)
	assert.NotNil(t, m.commandsDuration)
	assert.NotNil(t, m.eventsPersisted)
	assert.NotNil(t, m.projectionHandled)
	assert.NotNil(t, m.entitiesActive)
	assert.NotNil(t, m.projectionsActive)
	assert.NotNil(t, m.projectionLag)
	assert.NotNil(t, m.projectionOffset)
	assert.NotNil(t, m.projectionBehind)
}

func TestTelemetryFields(t *testing.T) {
	tracer := nooptrace.NewTracerProvider().Tracer("test")
	meter := noopmetric.NewMeterProvider().Meter("test")

	tel := &Telemetry{
		Tracer: tracer,
		Meter:  meter,
	}

	assert.NotNil(t, tel.Tracer)
	assert.NotNil(t, tel.Meter)
}
