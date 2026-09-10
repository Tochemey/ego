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

// sagaConfig holds the settings Engine.Saga collects from its SagaOption
// arguments for one saga.
type sagaConfig struct {
	// offsetRemoval reports whether the offsets the saga records while it
	// reads the journal are deleted once the saga completes or fails.
	offsetRemoval bool
}

// newSagaConfig applies the given options to the default saga settings.
func newSagaConfig(opts ...SagaOption) *sagaConfig {
	config := new(sagaConfig)
	for _, opt := range opts {
		opt.Apply(config)
	}

	return config
}

// SagaOption configures a saga started with Engine.Saga.
type SagaOption interface {
	// Apply sets the option value on the saga config.
	Apply(config *sagaConfig)
}

// ensures that the interface is fully implemented
var _ SagaOption = sagaOption(nil)

// sagaOption implements the SagaOption interface.
type sagaOption func(config *sagaConfig)

// Apply sets the option value on the saga config.
func (f sagaOption) Apply(config *sagaConfig) {
	f(config)
}

// WithOffsetRemoval makes eGo delete the offsets of the saga once it completes
// or fails.
//
// A saga is fed from the journal and records how far it has read in the offset
// store, one row per shard it read, under the projection name
// `ego.saga.<saga id>`. A settled saga never reads the journal again, so those
// rows have no reader; without this option they are kept, and the store grows
// with the number of sagas ever run. With it, the rows of a saga that reached
// SagaCompleted or SagaFailed are removed, and only its own: the offsets of
// projections are never touched.
//
// Deletion happens at settlement only. A saga that is still running when it is
// stopped, restarted or relocated always resumes from the offsets it recorded.
func WithOffsetRemoval() SagaOption {
	return sagaOption(func(config *sagaConfig) {
		config.offsetRemoval = true
	})
}
