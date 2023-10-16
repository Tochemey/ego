/*
 * Copyright (c) 2022-2023 Tochemey
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

package eventstream

import (
	"context"

	natsclient "github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/tochemey/goakt/log"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
)

// Publisher defines the Publisher connection
type Publisher struct {
	connection *natsclient.Conn
	logger     log.Logger
	connected  *atomic.Bool
}

// Publish publishes a message to a given topic
func (x *Publisher) Publish(ctx context.Context, topic string, message proto.Message) error {
	// add some logging information
	x.logger.Infof("publishing message=(%s) to topic=(%s)", message.ProtoReflect().Descriptor().FullName(), topic)
	// check whether the client is connected first
	if !x.connected.Load() {
		// add an error log
		x.logger.Error("events stream client is not connected")
		return ErrStreamPublisherNotConnected
	}

	// marshal message
	bytea, err := proto.Marshal(message)
	// handle the marshal error
	if err != nil {
		x.logger.Error(errors.Wrap(err, "failed to marshal message"))
		return err
	}

	// publish the message
	if err := x.connection.Publish(topic, bytea); err != nil {
		// add an error log
		x.logger.Error(errors.Wrapf(err, "failed to publish message=(%s) to topic=(%s)",
			message.ProtoReflect().Descriptor().FullName(), topic))
		return err
	}
	// add some logging information
	x.logger.Infof("message=(%s) successfully published to topic=(%s)", message.ProtoReflect().Descriptor().FullName(), topic)
	return nil
}

func (x *Publisher) Subscribe(topic string) {
	// add some logging information
	x.logger.Infof("adding a subscription to topic=(%s)", topic)
}

// Close closes the underlying connection
func (x *Publisher) Close() error {
	// add some logging information
	x.logger.Info("closing the events stream client connection")
	// check whether the connection is closed or not
	if !x.connected.Load() {
		// add an error log
		x.logger.Error("events stream client is not connected")
		return ErrStreamPublisherNotConnected
	}
	// close the client connection
	if err := x.connection.Drain(); err != nil {
		x.logger.Error(errors.Wrap(err, "failed to close the events stream client connection"))
		return err
	}
	x.logger.Info("events stream client connection is closed successfully")
	return nil
}
