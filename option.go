/*
 * MIT License
 *
 * Copyright (c) 2023-2025 Tochemey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ego

import (
	"time"

	"github.com/tochemey/goakt/v3/discovery"
	"github.com/tochemey/goakt/v3/log"

	"github.com/tochemey/ego/v3/internal/extensions"
	"github.com/tochemey/ego/v3/offsetstore"
	"github.com/tochemey/ego/v3/persistence"
	"github.com/tochemey/ego/v3/projection"
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
//   - logger: An instance of log.Logger used for logging system events and debugging information.
//
// Returns:
//   - Option: A functional option that configures the logger.
func WithLogger(logger log.Logger) Option {
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

// WithProjection configures the Engine to use a projection extension for processing persisted events.
// It sets up the projection handler along with buffering and recovery parameters.
//
// Parameters:
//   - handler: A projection.Handler implementation that defines how events are processed.
//   - bufferSize: The number of events to buffer in memory before processing.
//   - startOffset: The time from which to begin processing events.
//   - resetOffset: The fallback time to reset the offset in case of recovery or replay scenarios.
//   - pullInterval: The interval between polling the event store for new events.
//   - recovery: Optional recovery strategy that defines how the projection behaves on failure.
//
// Returns:
//   - Option: A functional option that applies the projection configuration to the Engine.
//
// Example usage:
//
//	engine := NewEngine(
//	    WithProjection(
//	        myHandler,
//	        100,
//	        time.Now().Add(-24*time.Hour),
//	        time.Time{},
//	        5*time.Second,
//	        projection.NewRecovery(WithRetries(3)),
//	    ),
//	)
func WithProjection(handler projection.Handler, bufferSize int, startOffset, resetOffset time.Time, pullInterval time.Duration, recovery *projection.Recovery) Option {
	return OptionFunc(func(e *Engine) {
		if recovery == nil {
			recovery = projection.NewRecovery()
		}
		e.projectionExtension = extensions.NewProjectionExtension(
			handler,
			bufferSize,
			startOffset,
			resetOffset,
			pullInterval,
			recovery,
		)
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
