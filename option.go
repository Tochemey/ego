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
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/supervisor"

	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/offsetstore"
	"github.com/tochemey/ego/v4/persistence"
	"github.com/tochemey/ego/v4/projection"
)

// Config captures every option an Engine needs.
//
// A Config is built once with NewConfig and reused twice: passed to
// goakt.NewActorSystem via GoaktOptions, then to NewEngine. Callers should
// not construct Config zero-values; use NewConfig and Option functions to
// populate it.
type Config struct {
	eventsStore   persistence.EventsStore
	stateStore    persistence.StateStore
	offsetStore   offsetstore.OffsetStore
	snapshotStore persistence.SnapshotStore
	logger        Logger
	projection    *projection.Options
	eventAdapters []eventadapter.EventAdapter
	telemetry     *Telemetry
	encryptor     encryption.Encryptor

	// eventStream is the in-process pub/sub stream eGo's entity actors
	// publish to and the engine's publishers/subscribers consume from. It is
	// allocated by NewConfig and the same instance is wired into the actor
	// system (as the EventsStream extension) by GoaktOptions and into the
	// engine by NewEngine.
	eventStream eventstream.Stream
}

// NewConfig builds a Config from a list of Options.
//
// eventsStore is the events store eGo persists event-sourced state to; pass
// nil for durable-state-only deployments that never host event-sourced
// entities.
//
// The returned Config is then passed to both goakt.NewActorSystem (via
// GoaktOptions) and NewEngine, so an actor system and the engine that plugs
// into it always share the same configuration.
func NewConfig(eventsStore persistence.EventsStore, opts ...Option) *Config {
	c := &Config{
		eventsStore: eventsStore,
		logger:      defaultLogger{},
		eventStream: eventstream.New(),
	}

	for _, opt := range opts {
		opt.Apply(c)
	}

	if isNilLogger(c.logger) {
		c.logger = defaultLogger{}
	}
	return c
}

// GoaktOptions returns the goakt.Options eGo requires when the caller
// constructs the actor system that hosts it.
//
// Pass the returned slice to goakt.NewActorSystem alongside any other
// goakt.Options the deployment needs (cluster, remote, TLS, custom
// extensions, …). eGo cannot register its extensions on an
// already-constructed actor system, so this handoff at construction time is
// the only supported entry point.
//
// The returned list always includes the events store, event stream, logger
// adapter, pub/sub option, and a default resume-on-error supervisor. It
// additionally contains the state store, offset store, projection, snapshot
// store, event adapters, telemetry, and encryptor extensions whenever the
// corresponding Option was set on this Config.
func (c *Config) GoaktOptions() []goakt.Option {
	opts := []goakt.Option{
		goakt.WithLogger(newLoggerAdapter(c.logger)),
		goakt.WithActorInitMaxRetries(5),
		goakt.WithPubSub(),
		goakt.WithDefaultSupervisor(
			supervisor.NewSupervisor(supervisor.WithAnyErrorDirective(supervisor.ResumeDirective)),
		),
		goakt.WithExtensions(
			extensions.NewEventsStore(c.eventsStore),
			extensions.NewEventsStream(c.eventStream),
		),
	}

	if c.stateStore != nil {
		opts = append(opts, goakt.WithExtensions(extensions.NewDurableStateStore(c.stateStore)))
	}

	if c.offsetStore != nil {
		opts = append(opts, goakt.WithExtensions(extensions.NewOffsetStore(c.offsetStore)))
	}

	if c.projection != nil {
		recovery := c.projection.Recovery
		if recovery == nil {
			recovery = projection.NewRecovery()
		}

		opts = append(opts, goakt.WithExtensions(extensions.NewProjectionExtension(
			c.projection.Handler,
			c.projection.BufferSize,
			c.projection.StartOffset,
			c.projection.ResetOffset,
			c.projection.PullInterval,
			recovery,
			c.projection.DeadLetterHandler,
		)))
	}

	if c.snapshotStore != nil {
		opts = append(opts, goakt.WithExtensions(extensions.NewSnapshotStore(c.snapshotStore)))
	}

	if len(c.eventAdapters) > 0 {
		opts = append(opts, goakt.WithExtensions(extensions.NewEventAdapters(c.eventAdapters)))
	}

	if c.telemetry != nil {
		opts = append(opts, goakt.WithExtensions(
			extensions.NewTelemetryExtension(c.telemetry.Tracer, c.telemetry.Meter)))
	}

	if c.encryptor != nil {
		opts = append(opts, goakt.WithExtensions(extensions.NewEncryptor(c.encryptor)))
	}

	return opts
}

// ClusterKinds returns the actor kinds eGo needs registered in the cluster
// configuration so that entity, durable-state, saga, and projection actors
// can be relocated across nodes.
//
// Plug them into goakt.NewClusterConfig().WithKinds(...) alongside any
// caller-defined kinds. Has no effect in single-node deployments.
func ClusterKinds() []goakt.Actor {
	return []goakt.Actor{
		new(EventSourcedActor),
		new(DurableStateActor),
		new(SagaActor),
		new(ProjectionActor),
	}
}

// Option configures a Config.
//
// Options are applied by NewConfig and surface through both GoaktOptions and
// NewEngine, ensuring the actor system and the engine share one source of
// truth.
type Option interface {
	Apply(c *Config)
}

var _ Option = OptionFunc(nil)

// OptionFunc is an adapter that turns a plain function into an Option.
type OptionFunc func(c *Config)

// Apply implements Option.
func (f OptionFunc) Apply(c *Config) {
	f(c)
}

// WithLogger sets the logger used by the engine and the goakt actor system
// it sits on.
//
// When unset, eGo uses a slog-backed default. The same logger is adapted
// into the goakt logger so the actor system, eGo's internals, and the caller
// log through one backend.
func WithLogger(logger Logger) Option {
	return OptionFunc(func(c *Config) {
		c.logger = logger
	})
}

// WithStateStore configures the store used to persist durable-state entities.
//
// A state store is mandatory for engines that host DurableStateBehavior
// entities. Event-sourced-only engines may omit it.
func WithStateStore(stateStore persistence.StateStore) Option {
	return OptionFunc(func(c *Config) {
		c.stateStore = stateStore
	})
}

// WithOffsetStore configures the offset store used by projections to durably
// track their processing position.
//
// An offset store is mandatory whenever WithProjection is used.
func WithOffsetStore(offsetStore offsetstore.OffsetStore) Option {
	return OptionFunc(func(c *Config) {
		c.offsetStore = offsetStore
	})
}

// WithProjection enables and configures the engine's projection runner.
//
// The supplied projection.Options carries the projection handler and
// runtime knobs:
//
//   - Handler is required; it processes each event the projection observes.
//   - BufferSize bounds the in-flight event window.
//   - StartOffset is the timestamp from which a new projection begins reading.
//   - ResetOffset is the fallback offset used when recovery rewinds the projection.
//   - PullInterval is how often the runner polls for new events.
//   - Recovery controls retry behavior; nil falls back to projection.NewRecovery().
//   - DeadLetterHandler receives events the projection cannot process after retries.
//
// A nil options pointer is ignored. Projections also require an offset store
// (WithOffsetStore) to track progress durably.
func WithProjection(options *projection.Options) Option {
	return OptionFunc(func(c *Config) {
		c.projection = options
	})
}

// WithSnapshotStore configures the store used to persist entity snapshots.
//
// When paired with WithSnapshotInterval on an entity, the engine
// periodically saves a snapshot of the entity's state so recovery can skip
// ahead instead of replaying the full event history. Event-sourced entities
// recover by full replay when no snapshot store is set.
func WithSnapshotStore(snapshotStore persistence.SnapshotStore) Option {
	return OptionFunc(func(c *Config) {
		c.snapshotStore = snapshotStore
	})
}

// WithTelemetry enables OpenTelemetry instrumentation on the engine.
//
// When configured, the engine emits trace spans and metrics for command
// processing, event persistence, and projection handling. Pass nil to
// disable instrumentation.
func WithTelemetry(telemetry *Telemetry) Option {
	return OptionFunc(func(c *Config) {
		c.telemetry = telemetry
	})
}

// WithEventAdapters registers one or more EventAdapter instances.
//
// Adapters transform persisted events from older schema versions into the
// shape the current code expects. They run transparently during entity
// recovery and projection consumption.
//
// Adapters are applied in registration order. When an event matches more
// than one adapter, the output of one feeds the next.
func WithEventAdapters(adapters ...eventadapter.EventAdapter) Option {
	return OptionFunc(func(c *Config) {
		c.eventAdapters = append(c.eventAdapters, adapters...)
	})
}

// WithEncryptor configures transparent encryption for event and snapshot
// payloads.
//
// Events are encrypted before persistence and decrypted during recovery and
// projection consumption. Combined with a key store, this enables
// GDPR-compliant crypto-shredding: deleting an entity's encryption key
// makes its events unrecoverable.
func WithEncryptor(encryptor encryption.Encryptor) Option {
	return OptionFunc(func(c *Config) {
		c.encryptor = encryptor
	})
}
