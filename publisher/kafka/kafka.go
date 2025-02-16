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

package kafka

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/tochemey/goakt/v3/log"
	"go.uber.org/atomic"

	"github.com/tochemey/ego/v3"
	"github.com/tochemey/ego/v3/egopb"
)

// EventsPublisher defines a Kafka publisher.
// This publisher is responsible for delivering ego events/durable state to a Kafka broker.
type EventsPublisher struct {
	config   *Config
	producer sarama.SyncProducer
	logger   log.Logger
	started  *atomic.Bool
}

// ensure EventsPublisher implements ego.EventPublisher.
var _ ego.EventPublisher = (*EventsPublisher)(nil)

// NewEventsPublisher creates a new instance of EventsPublisher.
// It requires a configuration instance to create the publisher.
//
// Parameters:
//   - config: The configuration for the Kafka publisher.
//     This configuration includes the Kafka broker addresses, TLS settings, and other options.
//
// Returns:
//   - *EventsPublisher: The new instance of EventsPublisher.
//   - error: An error if the publisher could not be created.
func NewEventsPublisher(config *Config) (*EventsPublisher, error) {
	saramaConfig := toSaramaConfig(config)
	producer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	return &EventsPublisher{
		config:   config,
		logger:   config.Logger,
		started:  atomic.NewBool(false),
		producer: producer,
	}, nil
}

// Close implements ego.EventPublisher.
func (x *EventsPublisher) Close(ctx context.Context) error {
	// we give the publisher 3 seconds to close. This is an abitrary value.
	// It helps to ensure that the publisher has enough time to close.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	x.started.Store(false)
	return x.producer.Close()
}

// ID implements ego.EventPublisher.
func (x *EventsPublisher) ID() string {
	return "eGo.Kafka.EventsPublisher"
}

// Publish implements ego.EventPublisher.
func (x *EventsPublisher) Publish(ctx context.Context, event *egopb.Event) error {
	// if !x.started.Load() {
	// 	return ego.ErrPublisherNotStarted
	// }
	panic("unimplemented")
}
