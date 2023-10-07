package ego

import (
	"github.com/tochemey/goakt/discovery"
	"github.com/tochemey/goakt/log"
	"github.com/tochemey/goakt/telemetry"
	"go.uber.org/atomic"
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
func WithCluster(discoProvider discovery.Provider, config discovery.Config, partitionsCount uint64) Option {
	return OptionFunc(func(e *Engine) {
		e.enableCluster = atomic.NewBool(true)
		e.discoveryProvider = discoProvider
		e.discoveryConfig = config
		e.partitionsCount = partitionsCount
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
