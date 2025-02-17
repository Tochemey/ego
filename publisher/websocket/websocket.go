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

package websocket

import (
	"context"
	"fmt"

	ws "github.com/gorilla/websocket"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3"
	"github.com/tochemey/ego/v3/egopb"
)

type EventsPublisher struct {
	config     *Config
	connection *ws.Conn
	started    *atomic.Bool
}

// ensure EventsPublisher implements ego.EventPublisher.
var _ ego.EventPublisher = (*EventsPublisher)(nil)

// NewEventsPublisher creates a new instance of EventsPublisher.
// It requires a configuration instance to create the publisher.
//
// Parameters:
//   - config: The configuration for the websocket publisher.
//     This configuration includes the websocket server address.
//
// Returns: The new instance of EventsPublisher.
func NewEventsPublisher(config *Config) (*EventsPublisher, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// TODO: add support for custom dialer
	dialer := ws.DefaultDialer
	conn, _, err := dialer.Dial(config.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the websocket server: %w", err)
	}

	return &EventsPublisher{
		config:     config,
		connection: conn,
		started:    atomic.NewBool(true),
	}, nil
}

// ID returns the ID of the publisher.
func (x *EventsPublisher) ID() string {
	return "ego-websocket"
}

// Close implements ego.EventPublisher.
func (x *EventsPublisher) Close(context.Context) error {
	x.started.Store(false)
	return x.connection.Close()
}

// Publish publishes an event to the websocket server.
// The event is serialized to byte array before being sent to the server.
//
// Parameters:
//   - ctx: The context of the operation.
//   - event: The event to publish.
//
// Returns: An error if the event cannot be published.
func (x *EventsPublisher) Publish(ctx context.Context, event *egopb.Event) error {
	if !x.started.Load() {
		return ego.ErrPublisherNotStarted
	}

	// serialize the event. No need to check for errors.
	payload, _ := proto.Marshal(event)
	return x.connection.WriteMessage(ws.BinaryMessage, payload)
}

type DurableStatePublisher struct {
	config     *Config
	connection *ws.Conn
	started    *atomic.Bool
}

// enforce compilation error
var _ ego.StatePublisher = (*DurableStatePublisher)(nil)

// NewDurableStatePublisher creates a new instance of DurableStatePublisher.
// It requires a configuration instance to create the publisher.
//
// Parameters:
//   - config: The configuration for the websocket publisher.
//     This configuration includes the websocket server address.
//
// Returns: The new instance of EventsPublisher.
func NewDurableStatePublisher(config *Config) (*DurableStatePublisher, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// TODO: add support for custom dialer
	dialer := ws.DefaultDialer
	conn, _, err := dialer.Dial(config.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the websocket server: %w", err)
	}

	return &DurableStatePublisher{
		config:     config,
		connection: conn,
		started:    atomic.NewBool(true),
	}, nil
}

// ID returns the ID of the publisher.
func (x *DurableStatePublisher) ID() string {
	return "ego-websocket"
}

// Close implements ego.EventPublisher.
func (x *DurableStatePublisher) Close(context.Context) error {
	x.started.Store(false)
	return x.connection.Close()
}

// Publish publishes a durable state to the websocket server.
// The durable state is serialized to byte array before being sent to the server.
//
// Parameters:
//   - ctx: The context of the operation.
//   - state: The durable state to publish.
//
// Returns: An error if the state cannot be published.
func (x *DurableStatePublisher) Publish(_ context.Context, state *egopb.DurableState) error {
	if !x.started.Load() {
		return ego.ErrPublisherNotStarted
	}

	// serialize the event. No need to check for errors.
	payload, _ := proto.Marshal(state)
	return x.connection.WriteMessage(ws.BinaryMessage, payload)
}
