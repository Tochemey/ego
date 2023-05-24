package ego

import (
	"github.com/tochemey/goakt/discovery"
	"github.com/tochemey/goakt/log"
	"github.com/tochemey/goakt/pkg/telemetry"
)

// Option is the interface that applies a configuration option.
type Option interface {
	// Apply sets the Option value of a config.
	Apply(e *Ego)
}

var _ Option = OptionFunc(nil)

// OptionFunc implements the Option interface.
type OptionFunc func(e *Ego)

func (f OptionFunc) Apply(e *Ego) {
	f(e)
}

// WithCluster enables cluster mode
func WithCluster(disco discovery.Discovery, remotingHost string, remotingPort int32) Option {
	return OptionFunc(func(e *Ego) {
		e.remotingHost = remotingHost
		e.remotingPort = remotingPort
		e.enableCluster = true
		e.discoveryMode = disco
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
