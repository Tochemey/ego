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
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Telemetry holds OpenTelemetry instrumentation for the engine.
// When nil fields are present, no instrumentation is performed for that signal.
type Telemetry struct {
	// Tracer is used to create spans for command processing, event persistence, and projection handling.
	Tracer trace.Tracer
	// Meter is used to record metrics for commands, events, and projections.
	Meter metric.Meter
}

// metrics holds pre-created metric instruments to avoid allocation on the hot path.
type metrics struct {
	commandsTotal     metric.Int64Counter
	commandsDuration  metric.Float64Histogram
	eventsPersisted   metric.Int64Counter
	projectionHandled metric.Int64Counter
	entitiesActive    metric.Int64UpDownCounter
	projectionsActive metric.Int64UpDownCounter
	projectionLag     metric.Int64Gauge
	projectionOffset  metric.Int64Gauge
	projectionBehind  metric.Int64Gauge
}

// newMetrics creates metric instruments from the given meter.
// Returns nil if meter is nil.
func newMetrics(meter metric.Meter) *metrics {
	if meter == nil {
		return nil
	}

	commandsTotal, _ := meter.Int64Counter("ego.commands.total",
		metric.WithDescription("Total number of commands processed"),
	)

	commandsDuration, _ := meter.Float64Histogram("ego.commands.duration",
		metric.WithDescription("Duration of command processing in milliseconds"),
	)

	eventsPersisted, _ := meter.Int64Counter("ego.events.persisted.total",
		metric.WithDescription("Total number of events persisted"),
	)

	projectionHandled, _ := meter.Int64Counter("ego.projection.events.processed.total",
		metric.WithDescription("Total number of events processed by projections"),
	)

	entitiesActive, _ := meter.Int64UpDownCounter("ego.entities.active",
		metric.WithDescription("Number of currently active entities"),
	)

	projectionsActive, _ := meter.Int64UpDownCounter("ego.projections.active",
		metric.WithDescription("Number of currently active projections"),
	)

	projectionLag, _ := meter.Int64Gauge("ego.projection.lag_ms",
		metric.WithDescription("Projection lag in milliseconds per shard"),
	)

	projectionOffset, _ := meter.Int64Gauge("ego.projection.latest_offset",
		metric.WithDescription("Current projection offset timestamp per shard"),
	)

	projectionBehind, _ := meter.Int64Gauge("ego.projection.events_behind",
		metric.WithDescription("Approximate number of unprocessed events per shard"),
	)

	return &metrics{
		commandsTotal:     commandsTotal,
		commandsDuration:  commandsDuration,
		eventsPersisted:   eventsPersisted,
		projectionHandled: projectionHandled,
		entitiesActive:    entitiesActive,
		projectionsActive: projectionsActive,
		projectionLag:     projectionLag,
		projectionOffset:  projectionOffset,
		projectionBehind:  projectionBehind,
	}
}
