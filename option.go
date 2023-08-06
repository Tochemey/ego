package ego

import (
	"github.com/tochemey/goakt/discovery"
	"github.com/tochemey/goakt/log"
	"github.com/tochemey/goakt/pkg/telemetry"
	"go.uber.org/atomic"
)

// Option is the interface that applies a configuration option.
type Option interface {
	// Apply sets the Option value of a config.
	Apply(e *Ego)
}

var _ Option = OptionFunc(nil)

// OptionFunc implements the Option interface.
type OptionFunc func(e *Ego)

// Apply applies the options to Ego
func (f OptionFunc) Apply(e *Ego) {
	f(e)
}

// WithCluster enables cluster mode
func WithCluster(discoProvider discovery.Provider, config discovery.Config, partitionsCount uint64) Option {
	return OptionFunc(func(e *Ego) {
		e.enableCluster = atomic.NewBool(true)
		e.discoveryProvider = discoProvider
		e.discoveryConfig = config
		e.partitionsCount = partitionsCount
	})
}

// WithLogger sets the logger
func WithLogger(logger log.Logger) Option {
	return OptionFunc(func(e *Ego) {
		e.logger = logger
	})
}

// WithTelemetry sets the telemetry engine
func WithTelemetry(telemetry *telemetry.Telemetry) Option {
	return OptionFunc(func(e *Ego) {
		e.telemetry = telemetry
	})
}
