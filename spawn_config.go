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

// spawnConfig defines the spawn config
type spawnConfig struct {
	// passivateAfter sets the passivation time
	// when this value is not defined then passivation is disabled
	passivateAfter time.Duration
	// specifies if the actor should be relocated
	toRelocate bool
	// supervisorDirective sets the supervisor directive to use
	// when the given entity fails
	supervisorDirective SupervisorDirective
}

// newSpawnConfig creates an instance of spawnConfig
func newSpawnConfig(opts ...SpawnOption) *spawnConfig {
	config := &spawnConfig{
		supervisorDirective: RestartDirective,
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

// WithPassivateAfter sets a custom duration after which an idle actor
// will be passivated. Passivation allows the actor system to free up
// resources by stopping actors that have been inactive for the specified
// duration. If the actor receives a message before this timeout,
// the passivation timer is reset.
func WithPassivateAfter(after time.Duration) SpawnOption {
	return spawnOption(func(config *spawnConfig) {
		config.passivateAfter = after
	})
}

// WithRelocation controls whether an actor should be relocated to another node in the cluster
// when its hosting node shuts down unexpectedly.
//
// In cluster mode, actors are relocatable by default to ensure system resilience and high availability.
// When relocation is enabled, the actor will be automatically redeployed on a healthy node if the original
// node becomes unavailable. This behavior is ideal for stateless or replicated actors that can resume
// execution without requiring node-specific context.
//
// Setting toRelocate to false disables this default behavior. Use this option when you require strict
// control over an actor's lifecycle or when the actor depends on node-specific resources, state, or hardware
// that cannot be replicated or recovered on another node.
//
// Parameters:
//   - toRelocate: If true, the actor is eligible for relocation on node failure.
//     If false, the actor will not be redeployed after a node shutdown.
//
// Returns: SpawnOption: A functional option that updates the actor's relocation configuration.
func WithRelocation(toRelocate bool) SpawnOption {
	return spawnOption(func(config *spawnConfig) {
		config.toRelocate = toRelocate
	})
}

// WithSupervisorDirective sets the SupervisorDirective that will be applied to the actor
// when it fails. This controls how the entities' failures are handled (e.g. restart, stop,
// escalate). If not provided, the default is RestartDirective.
// Use this to override the default supervision strategy for specific entities.
func WithSupervisorDirective(directive SupervisorDirective) SpawnOption {
	return spawnOption(func(config *spawnConfig) {
		config.supervisorDirective = directive
	})
}
