/*
 * MIT License
 *
 * Copyright (c) 2022-2025 Arsene Tochemey Gandote
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

package projection

import (
	"time"
)

// RecoveryPolicy defines the various policies to apply
// when a given projection panic
type RecoveryPolicy int

const (
	// Fail states that if the first attempt to invoke the handler fails
	// it will immediately give up and fail the projection
	Fail RecoveryPolicy = iota
	// RetryAndFail states that if the first attempt to invoke the handler fails it will retry invoking the handler with the
	// same envelope this number of `retries` with the `delay` between each attempt. It will give up
	// and fail the projection if all attempts fail. For this to work as expected one need to define the `retries` and `delay`
	// settings in the projection configuration.
	RetryAndFail
	// Skip states that if the first attempt to invoke the handler fails it will immediately give up, discard the envelope and
	// continue with next. This will commit the offset assuming the event has been successfully processed.
	// Use this strategy with care.
	Skip
	// RetryAndSkip states that if the first attempt to invoke the handler fails it will retry invoking the handler with the
	// same envelope this number of `retries` with the `delay` between each attempt. It will give up,
	// discard the element and continue with next if all attempts fail.
	// For this to work as expected one need to define the `retries` and `delay` settings in the projection configuration
	RetryAndSkip
)

// Recovery specifies the various recovery settings of a projection
// The option helps defines what happens when the projection handler fails to process
// the consumed event for a given persistence id
type Recovery struct {
	// retries specifies the number of times to retry handler function.
	// This is only applicable to `RetryAndFail` and `RetryAndSkip` recovery strategies
	// The default value is 5
	retries uint64
	// retryDelay specifies the delay between retry attempts
	// This is only applicable to `RetryAndFail` and `RetryAndSkip` recovery strategies
	// The default value is 1 second
	retryDelay time.Duration
	// strategy specifies strategy to use to recover from unhandled exceptions without causing the projection to fail
	policy RecoveryPolicy
}

// NewRecovery creates an instance of Recovery
func NewRecovery(options ...RecoveryOption) *Recovery {
	// create the recovery object with the default values
	recovery := &Recovery{
		retries:    5,
		retryDelay: time.Second,
		policy:     Fail,
	}
	// apply the various options
	for _, opt := range options {
		opt.Apply(recovery)
	}
	// return the created recovery object
	return recovery
}

// Retries returns the number of times to retry handler function.
func (c Recovery) Retries() uint64 {
	return c.retries
}

// RetryDelay returns the delay between retry attempts
func (c Recovery) RetryDelay() time.Duration {
	return c.retryDelay
}

// RecoveryPolicy returns the recovery policy
func (c Recovery) RecoveryPolicy() RecoveryPolicy {
	return c.policy
}

// RecoveryOption is the interface that applies a recovery option.
type RecoveryOption interface {
	// Apply sets the RecoveryOption value of a recovery.
	Apply(recovery *Recovery)
}

var _ RecoveryOption = RecoveryOptionFunc(nil)

// RecoveryOptionFunc implements the RecoveryOption interface.
type RecoveryOptionFunc func(recovery *Recovery)

func (f RecoveryOptionFunc) Apply(c *Recovery) {
	f(c)
}

// WithRetries sets the number of retries
func WithRetries(retries uint64) RecoveryOption {
	return RecoveryOptionFunc(func(recovery *Recovery) {
		recovery.retries = retries
	})
}

// WithRetryDelay sets the retry delay
func WithRetryDelay(delay time.Duration) RecoveryOption {
	return RecoveryOptionFunc(func(recovery *Recovery) {
		recovery.retryDelay = delay
	})
}

// WithRecoveryPolicy sets the recovery policy
func WithRecoveryPolicy(policy RecoveryPolicy) RecoveryOption {
	return RecoveryOptionFunc(func(recovery *Recovery) {
		recovery.policy = policy
	})
}
