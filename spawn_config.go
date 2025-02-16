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
}

// newSpawnConfig creates an instance of spawnConfig
func newSpawnConfig(opts ...SpawnOption) *spawnConfig {
	config := new(spawnConfig)
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
