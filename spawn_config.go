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

import "time"

// EntitiesPlacement defines the algorithm used by the entity system to determine
// where an entity should be spawned in a clustered environment.
//
// This strategy is only relevant when cluster mode is enabled.
// It affects how entities are distributed across the nodes in the cluster.
type EntitiesPlacement int

const (
	// RoundRobin distributes entities evenly across nodes
	// by cycling through the available nodes in a round-robin manner.
	// This strategy provides balanced load distribution over time.
	// ⚠️ Note: This strategy is subject to the cluster topology at the time of creation. For a stable cluster topology,
	// it ensures an even distribution of entities across all nodes.
	RoundRobin EntitiesPlacement = iota

	// Random selects a node at random from the available pool of nodes.
	// This strategy is stateless and can help quickly spread entities across the cluster,
	// but may result in uneven load distribution.
	Random

	// Local forces the entity to be spawned on the local node,
	// regardless of the cluster configuration.
	// Useful when locality is important (e.g., accessing local resources).
	Local

	// LeastLoad selects the node with the least current load to spawn the entity.
	// This strategy aims to optimize resource utilization by placing entities
	// on nodes that are less busy, potentially improving performance and responsiveness.
	// Note: This strategy may require additional overhead when placing entities,
	// as it needs to get nodes load metrics depending on the cluster size.
	LeastLoad
)

// spawnConfig defines the spawn config
type spawnConfig struct {
	// passivateAfter sets the passivation time
	// when this value is not defined then passivation is disabled
	passivateAfter time.Duration
	// specifies if the entity should be relocated
	toRelocate bool
	// supervisorDirective sets the supervisor directive to use
	// when the given entity fails
	supervisorDirective SupervisorDirective
	// specifies the placement strategy to use
	entitiesPlacement EntitiesPlacement
}

// newSpawnConfig creates an instance of spawnConfig
func newSpawnConfig(opts ...SpawnOption) *spawnConfig {
	config := &spawnConfig{
		supervisorDirective: RestartDirective,
		entitiesPlacement:   RoundRobin,
	}
	for _, opt := range opts {
		opt.Apply(config)
	}
	return config
}

// SpawnOption is the interface that applies to
type SpawnOption interface {
	// Apply sets the Option value of a config.
	Apply(config *spawnConfig)
}

// ensures that the interface is fully implemented
var _ SpawnOption = spawnOption(nil)

// spawnOption implements the SpawnOption interface.
type spawnOption func(config *spawnConfig)

// Apply sets the Option value of a config.
func (f spawnOption) Apply(c *spawnConfig) {
	f(c)
}

// WithPassivateAfter sets a custom duration after which an idle entity
// will be passivated. Passivation allows the entity system to free up
// resources by stopping entities that have been inactive for the specified
// duration. If the entity receives a message before this timeout,
// the passivation timer is reset.
func WithPassivateAfter(after time.Duration) SpawnOption {
	return spawnOption(func(config *spawnConfig) {
		config.passivateAfter = after
	})
}

// WithRelocation controls whether an entity should be relocated to another node in the cluster
// when its hosting node shuts down unexpectedly.
//
// In cluster mode, entities are relocatable by default to ensure system resilience and high availability.
// When relocation is enabled, the entity will be automatically redeployed on a healthy node if the original
// node becomes unavailable. This behavior is ideal for stateless or replicated entities that can resume
// execution without requiring node-specific context.
//
// Setting toRelocate to false disables this default behavior. Use this option when you require strict
// control over an entity's lifecycle or when the entity depends on node-specific resources, state, or hardware
// that cannot be replicated or recovered on another node.
//
// Parameters:
//   - toRelocate: If true, the entity is eligible for relocation on node failure.
//     If false, the entity will not be redeployed after a node shutdown.
//
// Returns: SpawnOption: A functional option that updates the entity's relocation configuration.
func WithRelocation(toRelocate bool) SpawnOption {
	return spawnOption(func(config *spawnConfig) {
		config.toRelocate = toRelocate
	})
}

// WithSupervisorDirective sets the SupervisorDirective that will be applied to the entity
// when it fails. This controls how the entities' failures are handled (e.g. restart, stop,
// escalate). If not provided, the default is RestartDirective.
// Use this to override the default supervision strategy for specific entities.
func WithSupervisorDirective(directive SupervisorDirective) SpawnOption {
	return spawnOption(func(config *spawnConfig) {
		config.supervisorDirective = directive
	})
}

// WithPlacement returns a SpawnOption that sets the placement strategy to be used when spawning an entity
// in cluster mode via the SpawnOn function.
//
// This option determines how the entity system selects a target node for spawning
// the entity across the cluster. Valid strategies include RoundRobin, Random, and Local.
//
// Parameters:
//   - placement: A EntitiesPlacement value specifying how to distribute the entity.
//
// Returns:
//   - SpawnOption that sets the placement strategy in the spawn configuration.
func WithPlacement(placement EntitiesPlacement) SpawnOption {
	return spawnOption(func(config *spawnConfig) {
		config.entitiesPlacement = placement
	})
}
