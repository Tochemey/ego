package projection

import (
	"time"

	"github.com/tochemey/ego/egopb"
)

// Recovery specifies the various recovery settings of a projection
// The option helps defines what happens when the projection handler fails to process
// the consumed event for a given persistence ID
type Recovery struct {
	// retries specifies the number of times to retry handler function.
	// This is only applicable to `RETRY_AND_FAIL` and `RETRY_AND_SKIP` recovery strategies
	// The default value is 5
	retries uint64
	// retryDelay specifies the delay between retry attempts
	// This is only applicable to `RETRY_AND_FAIL` and `RETRY_AND_SKIP` recovery strategies
	// The default value is 1 second
	retryDelay time.Duration
	// strategy specifies strategy to use to recover from unhandled exceptions without causing the projection to fail
	strategy egopb.ProjectionRecoveryStrategy
}

// NewRecovery creates an instance of Recovery
func NewRecovery(options ...Option) *Recovery {
	cfg := &Recovery{
		retries:    5,
		retryDelay: time.Second,
		strategy:   egopb.ProjectionRecoveryStrategy_FAIL,
	}
	// apply the various options
	for _, opt := range options {
		opt.Apply(cfg)
	}

	return cfg
}

// Retries returns the number of times to retry handler function.
func (c Recovery) Retries() uint64 {
	return c.retries
}

// RetryDelay returns the delay between retry attempts
func (c Recovery) RetryDelay() time.Duration {
	return c.retryDelay
}

// RecoveryStrategy returns the recovery strategy
func (c Recovery) RecoveryStrategy() egopb.ProjectionRecoveryStrategy {
	return c.strategy
}

// Option is the interface that applies a recoveryuration option.
type Option interface {
	// Apply sets the Option value of a recovery.
	Apply(recovery *Recovery)
}

var _ Option = OptionFunc(nil)

// OptionFunc implements the Option interface.
type OptionFunc func(recovery *Recovery)

func (f OptionFunc) Apply(c *Recovery) {
	f(c)
}

// WithRetries sets the number of retries
func WithRetries(retries uint64) Option {
	return OptionFunc(func(recovery *Recovery) {
		recovery.retries = retries
	})
}

// WithRetryDelay sets the retry delay
func WithRetryDelay(delay time.Duration) Option {
	return OptionFunc(func(recovery *Recovery) {
		recovery.retryDelay = delay
	})
}

// WithRecoveryStrategy sets the recovery strategy
func WithRecoveryStrategy(strategy egopb.ProjectionRecoveryStrategy) Option {
	return OptionFunc(func(recovery *Recovery) {
		recovery.strategy = strategy
	})
}
