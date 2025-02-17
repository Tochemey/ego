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

package nats

import (
	"context"

	"github.com/flowchartsman/retry"
	"github.com/nats-io/nats.go"
	"github.com/tochemey/goakt/v3/log"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3"
	"github.com/tochemey/ego/v3/egopb"
)

// EventsPublisher defines a NATS publisher.
// This publisher is responsible for delivering ego events to a NATS server.
type EventsPublisher struct {
	config     *Config
	logger     log.Logger
	started    *atomic.Bool
	connection *nats.Conn
	jetStream  nats.JetStream
}

// ensure EventsPublisher implements ego.EventPublisher.
var _ ego.EventPublisher = (*EventsPublisher)(nil)

// NewEventsPublisher creates a new instance of EventsPublisher.
// It requires a configuration instance to create the publisher.
//
// Parameters:
//   - config: The configuration for the NATS publisher.
//     This configuration includes the NATS server addresses, TLS settings, and other options.
//
// Returns: The new instance of EventsPublisher.
func NewEventsPublisher(config *Config) (*EventsPublisher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// TODO: add additional configuration options
	opts := nats.GetDefaultOptions()
	opts.Url = config.NatsServer
	opts.ReconnectWait = config.ReconnectWait
	opts.MaxReconnect = -1

	var (
		connection *nats.Conn
		err        error
	)

	// let us connect using an exponential backoff mechanism
	// create a new instance of retrier that will try a maximum of five times, with
	// an initial delay and a maximum delay of opts.ReconnectWait
	retrier := retry.NewRetrier(config.MaxJoinAttempts, opts.ReconnectWait, opts.ReconnectWait)
	err = retrier.Run(func() error {
		connection, err = opts.Connect()
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// create an instance of JetStream
	// TODO: add additional configuration options
	jetStream, err := connection.JetStream()
	if err != nil {
		return nil, err
	}

	_, err = jetStream.AddStream(&nats.StreamConfig{
		Name:     "ego-events",
		Subjects: []string{config.NatsSubject},
	})
	if err != nil {
		return nil, err
	}

	return &EventsPublisher{
		config:     config,
		logger:     config.Logger,
		started:    atomic.NewBool(true),
		connection: connection,
		jetStream:  jetStream,
	}, nil
}

// Close closes the NATS publisher.
// It stops the publisher from sending events to the NATS server.
//
// Parameters:
//   - ctx: The context to close the publisher.
//
// Returns: An error if the publisher fails to close.
func (x *EventsPublisher) Close(context.Context) error {
	x.started.Store(false)
	if err := x.connection.Drain(); err != nil {
		return err
	}
	x.connection.Close()
	return nil
}

// ID returns the ID of the NATS publisher.
func (x *EventsPublisher) ID() string {
	return "eGo.NATS.EventsPublisher"
}

// Publish sends an event to the NATS server.
//
// Parameters:
//   - ctx: The context to send the event.
//   - event: The event to send.
//
// Returns: An error if the event fails to be sent.
func (x *EventsPublisher) Publish(_ context.Context, event *egopb.Event) error {
	if !x.started.Load() {
		return ego.ErrPublisherNotStarted
	}

	// serialize the event. No need to check for errors.
	payload, _ := proto.Marshal(event)

	_, err := x.jetStream.Publish(x.config.NatsSubject, payload)
	if err != nil {
		return err
	}
	return nil
}

type DurableStatePublisher struct {
	config     *Config
	logger     log.Logger
	started    *atomic.Bool
	connection *nats.Conn
	jetStream  nats.JetStream
}

// ensure DurableStatesPublisher implements ego.DurableStatePublisher.
var _ ego.StatePublisher = (*DurableStatePublisher)(nil)

// NewDurableStatePublisher creates a new instance of DurableStatePublisher.
// It requires a configuration instance to create the publisher.
//
// Parameters:
//   - config: The configuration for the NATS publisher.
//     This configuration includes the NATS server addresses, TLS settings, and other options.
//
// Returns: The new instance of DurableStatePublisher.
func NewDurableStatePublisher(config *Config) (*DurableStatePublisher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// TODO: add additional configuration options
	opts := nats.GetDefaultOptions()
	opts.Url = config.NatsServer
	opts.ReconnectWait = config.ReconnectWait
	opts.MaxReconnect = -1

	var (
		connection *nats.Conn
		err        error
	)

	// let us connect using an exponential backoff mechanism
	// create a new instance of retrier that will try a maximum of five times, with
	// an initial delay and a maximum delay of opts.ReconnectWait
	retrier := retry.NewRetrier(config.MaxJoinAttempts, opts.ReconnectWait, opts.ReconnectWait)
	err = retrier.Run(func() error {
		connection, err = opts.Connect()
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// create an instance of JetStream
	// TODO: add additional configuration options
	jetStream, err := connection.JetStream()
	if err != nil {
		return nil, err
	}

	_, err = jetStream.AddStream(&nats.StreamConfig{
		Name:     "ego-durable-states",
		Subjects: []string{config.NatsSubject},
	})
	if err != nil {
		return nil, err
	}

	return &DurableStatePublisher{
		config:     config,
		logger:     config.Logger,
		started:    atomic.NewBool(true),
		connection: connection,
		jetStream:  jetStream,
	}, nil
}

// Close closes the NATS publisher.
// It stops the publisher from sending events to the NATS server.
//
// Parameters:
//   - ctx: The context to close the publisher.
//
// Returns: An error if the publisher fails to close.
func (e *DurableStatePublisher) Close(context.Context) error {
	e.started.Store(false)
	if err := e.connection.Drain(); err != nil {
		return err
	}
	e.connection.Close()
	return nil
}

// ID returns the ID of the NATS publisher.
func (e *DurableStatePublisher) ID() string {
	return "eGo.NATS.DurableStatePublisher"
}

// Publish sends a message to the NATS server.
//
// Parameters:
//   - ctx: The context to send the event.
//   - state: The durable state to send.
//
// Returns: An error if the event fails to be sent.
func (e *DurableStatePublisher) Publish(_ context.Context, state *egopb.DurableState) error {
	if !e.started.Load() {
		return ego.ErrPublisherNotStarted
	}

	// serialize the event. No need to check for errors.
	payload, _ := proto.Marshal(state)

	_, err := e.jetStream.Publish(e.config.NatsSubject, payload)
	if err != nil {
		return err
	}
	return nil
}
