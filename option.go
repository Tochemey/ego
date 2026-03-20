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
	"github.com/tochemey/goakt/v4/discovery"

	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/offsetstore"
	"github.com/tochemey/ego/v4/persistence"
	"github.com/tochemey/ego/v4/projection"
)

// Option defines a configuration option that can be applied to a Engine.
//
// Implementations of this interface modify the Engine's configuration when applied.
type Option interface {
	// Apply sets the Option value of a config.
	Apply(e *Engine)
}

var _ Option = OptionFunc(nil)

// OptionFunc is a function type that implements the Option interface.
//
// It allows functions to be used as configuration options for Engine.
type OptionFunc func(e *Engine)

// Apply applies the OptionFunc to the given Engine.
//
// This enables the use of functions as dynamic configuration options.
func (f OptionFunc) Apply(e *Engine) {
	f(e)
}

// WithCluster enables cluster mode by configuring the necessary parameters
// for distributed communication and peer discovery.
//
// Parameters:
//   - provider: The discovery.Provider responsible for peers discovery in the cluster.
//   - partitionCount: The number of partitions used for distributing data across the cluster.
//   - minimumPeersQuorum: The minimum number of peers required to form a quorum.
//   - host: The hostname or IP address of the current node.
//   - remotingPort: The port used for remote actor communication.
//   - discoveryPort: The port used for service discovery.
//   - peersPort: The port used for peer-to-peer communication.
//
// Returns:
//   - Option: A functional option that configures the cluster settings.
func WithCluster(provider discovery.Provider, partitionCount uint64, minimumPeersQuorum uint16, host string, remotingPort, discoveryPort, peersPort int) Option {
	return OptionFunc(func(e *Engine) {
		e.clusterEnabled.Store(true)
		e.discoveryProvider = provider
		e.partitionsCount = partitionCount
		e.peersPort = peersPort
		e.minimumPeersQuorum = minimumPeersQuorum
		e.discoveryPort = discoveryPort
		e.bindAddr = host
		e.remotingPort = remotingPort
	})
}

// WithLogger sets the logger for the system, allowing custom logging implementations.
//
// Parameters:
//   - logger: An instance of Logger used for logging system events and debugging information.
//
// Returns:
//   - Option: A functional option that configures the logger.
func WithLogger(logger Logger) Option {
	return OptionFunc(func(e *Engine) {
		e.logger = logger
	})
}

// WithStateStore sets the durable state store for persisting entity state.
// This is necessary when creating a durable state entity to ensure state
// survives restarts and failures.
//
// Parameters:
//   - stateStore: An instance of persistence.StateStore responsible for storing entity state durably.
//
// Returns:
//   - Option: A functional option that configures the state store.
func WithStateStore(stateStore persistence.StateStore) Option {
	return OptionFunc(func(e *Engine) {
		e.stateStore = stateStore
	})
}

// WithOffsetStore sets a custom offset store to the Engine for tracking the processing position
// of projections.
//
// An offset store is responsible for persisting and retrieving the last processed offset,
// enabling reliable and resumable event processing across restarts or failures.
// This option allows plugging in a custom implementation of persistence.OffsetStore,
// which can be backed by a database, message queue metadata, or any other durable mechanism.
//
// Parameters:
//   - offsetStore: An implementation of the persistence.OffsetStore interface
//     used to persist and retrieve offset positions.
//
// Returns:
//   - Option: A functional option that applies the custom offset store to the Engine.
//
// Example:
//
//	engine := NewEngine(
//	    WithOffsetStore(myOffsetStore),
//	)
func WithOffsetStore(offsetStore offsetstore.OffsetStore) Option {
	return OptionFunc(func(e *Engine) {
		e.offsetStore = offsetStore
	})
}

// WithTLS configures TLS settings for both the server and client, ensuring
// secure communication through encryption and authentication.
//
// Ensure that both the server and client are configured with the same
// root Certificate Authority (CA) to enable a successful handshake and
// mutual authentication.
//
// In cluster mode, all nodes must share the same root CA to establish
// secure communication and complete handshakes successfully.
//
// Parameters:
//   - tls: A pointer to a TLS configuration struct that contains the
//     client and server TLS settings.
//
// Returns:
//   - Option: A functional option that configures TLS settings.
func WithTLS(tls *TLS) Option {
	return OptionFunc(func(e *Engine) {
		e.tls = tls
	})
}

// WithProjection configures the Engine's projection extension using the
// provided projection.Options.
//
// Projections consume persisted events and update a read model or external
// system. This option wires the projection handler, buffering, offsets, polling,
// and recovery strategy.
//
// Behavior and defaults:
//   - Handler (required): The projection.Handler that processes events.
//   - BufferSize: Number of events buffered in memory before processing.
//   - StartOffset: Initial offset time to start reading events from.
//   - ResetOffset: Fallback offset time used when recovery requires a rewind.
//   - PullInterval: How often to poll for new events.
//   - Recovery: If nil, defaults to projection.NewRecovery().
//
// Offset management:
//   - Offsets are persisted via the Engine’s OffsetStore (configured with WithOffsetStore).
//     If no OffsetStore is provided, projections cannot durably track progress.
//
// Typical usage:
//   - Use WithProjection to centralize configuration.
//   - Prefer this over the deprecated WithProjection helper.
//
// Example:
//
//	engine := NewEngine(
//	    WithOffsetStore(myOffsetStore),
//	    WithProjection(projection.Options{
//	        Handler:      myHandler,
//	        BufferSize:   256,
//	        StartOffset:  time.Now().Add(-24 * time.Hour),
//	        ResetOffset:  time.Time{}, // zero means "no explicit reset fallback"
//	        PullInterval: 2 * time.Second,
//	        Recovery:     projection.NewRecovery(projection.WithRetries(3)),
//	    }),
//	)
func WithProjection(options *projection.Options) Option {
	return OptionFunc(func(e *Engine) {
		if options != nil {
			recovery := options.Recovery
			if recovery == nil {
				recovery = projection.NewRecovery()
			}
			e.projectionExtension = extensions.NewProjectionExtension(
				options.Handler,
				options.BufferSize,
				options.StartOffset,
				options.ResetOffset,
				options.PullInterval,
				recovery,
				options.DeadLetterHandler,
			)
		}
	})
}

// WithSnapshotStore sets the snapshot store for persisting entity state snapshots.
// When configured alongside WithSnapshotInterval on an entity, the engine will
// periodically save state snapshots to speed up recovery.
// Without a snapshot store, event-sourced entities recover by replaying all events.
//
// Parameters:
//   - snapshotStore: An instance of persistence.SnapshotStore responsible for storing snapshots.
//
// Returns:
//   - Option: A functional option that configures the snapshot store.
func WithSnapshotStore(snapshotStore persistence.SnapshotStore) Option {
	return OptionFunc(func(e *Engine) {
		e.snapshotStore = snapshotStore
	})
}

// WithTelemetry configures OpenTelemetry instrumentation for the engine.
//
// When set, the engine will emit tracing spans and metrics for command processing,
// event persistence, and projection handling. When nil, no instrumentation is performed.
//
// Parameters:
//   - telemetry: A pointer to a Telemetry configuration struct containing the tracer and meter.
//
// Returns:
//   - Option: A functional option that configures telemetry.
func WithTelemetry(telemetry *Telemetry) Option {
	return OptionFunc(func(e *Engine) {
		e.telemetry = telemetry
	})
}

// WithEventAdapters registers one or more EventAdapter instances with the engine.
//
// Event adapters transform persisted events from older schema versions into the
// current expected shape. They are applied transparently during entity recovery
// (event replay) and projection consumption.
//
// Adapters are applied in registration order. Each adapter inspects the event
// and decides whether to transform it. If multiple adapters match, they are
// chained: the output of one becomes the input of the next.
//
// Parameters:
//   - adapters: One or more EventAdapter implementations to register.
//
// Returns:
//   - Option: A functional option that configures the event adapters.
func WithEventAdapters(adapters ...eventadapter.EventAdapter) Option {
	return OptionFunc(func(e *Engine) {
		e.eventAdapters = append(e.eventAdapters, adapters...)
	})
}

// WithEncryptor configures transparent encryption for event and snapshot payloads.
// When set, events are encrypted before persistence and decrypted during recovery
// and projection consumption. This enables GDPR crypto-shredding via the underlying
// key store: deleting an entity's encryption key makes its events irrecoverable.
//
// Parameters:
//   - encryptor: An implementation of encryption.Encryptor that handles encrypt/decrypt.
//
// Returns:
//   - Option: A functional option that configures the encryptor.
func WithEncryptor(encryptor encryption.Encryptor) Option {
	return OptionFunc(func(e *Engine) {
		e.encryptor = encryptor
	})
}

// WithRoles sets the roles advertised by this node.
//
// A role is a label/metadata used by the cluster to define a node’s
// responsibilities (e.g., "web", "entity", "projection"). Not all nodes
// need to run the same workloads—roles let you dedicate nodes to specific
// purposes such as the web front-end, data access layer, or background
// processing.
//
// In practice, nodes with the "entity" role run actors/services such as
// persistent entities, while nodes with the "projection" role run read-side
// projections. This lets you scale parts of your application independently
// and optimize resource usage.
//
// Once roles are set, you can use SpawnOn("<role>") to spawn an actor on a
// node that advertises that role.
//
// This call replaces any previously configured roles. Duplicates are
// de-duplicated; order is not meaningful
func WithRoles(roles ...string) Option {
	return OptionFunc(func(e *Engine) {
		e.roles.Append(roles...)
	})
}
