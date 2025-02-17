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

package pulsar

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3"
	"github.com/tochemey/ego/v3/egopb"
)

// EventsPublisher defines a Pulsar publisher.
// This publisher is responsible for delivering ego events to a Pulsar server.
type EventsPublisher struct {
	client   pulsar.Client
	producer pulsar.Producer
	started  *atomic.Bool
}

// ensure EventsPublisher implements ego.EventPublisher.
var _ ego.EventPublisher = (*EventsPublisher)(nil)

// NewEventsPublisher creates a new instance of EventsPublisher.
//
// Parameters:
//   - clientOptions: The client options for the Pulsar client.
//   - producerOptions: The producer options for the Pulsar producer.
//
// Returns: The new instance of EventsPublisher or an error if the publisher cannot be created.
func NewEventsPublisher(clientOptions *pulsar.ClientOptions,
	producerOptions *pulsar.ProducerOptions) (*EventsPublisher, error) {
	// create a new Pulsar client
	client, err := pulsar.NewClient(*clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	// create a new Pulsar producer
	producer, err := client.CreateProducer(*producerOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &EventsPublisher{
		client:   client,
		producer: producer,
		started:  atomic.NewBool(true),
	}, nil
}

// ID returns the publisher ID.
func (x *EventsPublisher) ID() string {
	return "eGo.Pulsar.EventsPublisher"
}

// Publish publishes an event to the Pulsar server.
//
// Parameters:
//   - ctx: The context to use for publishing the event.
//   - event: The event to publish.
//
// Returns: An error if the event cannot be published.
func (x *EventsPublisher) Publish(ctx context.Context, event *egopb.Event) error {
	if !x.started.Load() {
		return ego.ErrPublisherNotStarted
	}

	// serialize the event. No need to check for errors.
	payload, _ := proto.Marshal(event)
	_, err := x.producer.Send(ctx, &pulsar.ProducerMessage{
		Key:         event.GetPersistenceId(),
		OrderingKey: event.GetPersistenceId(),
		Payload:     payload,
	})
	return err
}

// Close closes the publisher.
// It releases all resources associated with the publisher.
//
// Parameters:
//   - ctx: The context to use for closing the publisher.
//
// Returns: An error if the publisher cannot be closed.
func (x *EventsPublisher) Close(context.Context) error {
	x.started.Store(false)
	x.producer.Close()
	x.client.Close()
	return nil
}

// EventsPublisher defines a Pulsar publisher.
// This publisher is responsible for delivering ego events to a Pulsar server.
type DurableStatePublisher struct {
	client   pulsar.Client
	producer pulsar.Producer
	started  *atomic.Bool
}

// ensure DurableStatePublisher implements ego.StatePublisher.
var _ ego.StatePublisher = (*DurableStatePublisher)(nil)

// NewDurableStatePublisher creates a new instance of DurableStatePublisher.
//
// Parameters:
//   - clientOptions: The client options for the Pulsar client.
//   - producerOptions: The producer options for the Pulsar producer.
//
// Returns: The new instance of EventsPublisher or an error if the publisher cannot be created.
func NewDurableStatePublisher(clientOptions *pulsar.ClientOptions,
	producerOptions *pulsar.ProducerOptions) (*DurableStatePublisher, error) {
	// create a new Pulsar client
	client, err := pulsar.NewClient(*clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	// create a new Pulsar producer
	producer, err := client.CreateProducer(*producerOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &DurableStatePublisher{
		client:   client,
		producer: producer,
		started:  atomic.NewBool(true),
	}, nil
}

// ID returns the publisher ID.
func (x *DurableStatePublisher) ID() string {
	return "eGo.Pulsar.EventsPublisher"
}

// Publish publishes an event to the Pulsar server.
//
// Parameters:
//   - ctx: The context to use for publishing the event.
//   - event: The event to publish.
//
// Returns: An error if the event cannot be published.
func (x *DurableStatePublisher) Publish(ctx context.Context, state *egopb.DurableState) error {
	if !x.started.Load() {
		return ego.ErrPublisherNotStarted
	}

	// serialize the event. No need to check for errors.
	payload, _ := proto.Marshal(state)
	_, err := x.producer.Send(ctx, &pulsar.ProducerMessage{
		Key:         state.GetPersistenceId(),
		OrderingKey: state.GetPersistenceId(),
		Payload:     payload,
	})
	return err
}

// Close closes the publisher.
// It releases all resources associated with the publisher.
//
// Parameters:
//   - ctx: The context to use for closing the publisher.
//
// Returns: An error if the publisher cannot be closed.
func (x *DurableStatePublisher) Close(context.Context) error {
	x.started.Store(false)
	x.producer.Close()
	x.client.Close()
	return nil
}
