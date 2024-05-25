/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
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
	"go.uber.org/atomic"

	"github.com/tochemey/goakt/v2/discovery"
	"github.com/tochemey/goakt/v2/log"
	"github.com/tochemey/goakt/v2/telemetry"
)

// Option is the interface that applies a configuration option.
type Option interface {
	// Apply sets the Option value of a config.
	Apply(e *Engine)
}

var _ Option = OptionFunc(nil)

// OptionFunc implements the Option interface.
type OptionFunc func(e *Engine)

// Apply applies the options to Engine
func (f OptionFunc) Apply(e *Engine) {
	f(e)
}

// WithCluster enables cluster mode
func WithCluster(provider discovery.Provider, partitionCount uint64, minimumPeersQuorum uint16, host string, remotingPort, gossipPort, peersPort int) Option {
	return OptionFunc(func(e *Engine) {
		e.enableCluster = atomic.NewBool(true)
		e.discoveryProvider = provider
		e.partitionsCount = partitionCount
		e.peersPort = peersPort
		e.minimumPeersQuorum = minimumPeersQuorum
		e.gossipPort = gossipPort
		e.hostName = host
		e.remotingPort = remotingPort
	})
}

// WithLogger sets the logger
func WithLogger(logger log.Logger) Option {
	return OptionFunc(func(e *Engine) {
		e.logger = logger
	})
}

// WithTelemetry sets the telemetry engine
func WithTelemetry(telemetry *telemetry.Telemetry) Option {
	return OptionFunc(func(e *Engine) {
		e.telemetry = telemetry
	})
}
