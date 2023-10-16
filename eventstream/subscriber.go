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
	"github.com/tochemey/goakt/log"
)

// MessageHandler Handle processes the received message.
type MessageHandler func(ctx context.Context, data []byte) error

type Subscriber struct {
	// underlying NATs connection
	connection *natsclient.Conn
	// underlying NATs subscription
	subscription *natsclient.Subscription
	// logger
	logger log.Logger
}

// NewSubscriber creates an instance of Subscriber
func NewSubscriber(connection *natsclient.Conn, logger log.Logger) *Subscriber {
	return &Subscriber{
		connection: connection,
		logger:     logger,
	}
}

// Consume receives messages from the topic and pass it to
// the message handler and the buffered channel to keep track of errors
func (x *Subscriber) Consume(ctx context.Context, topic string, handler MessageHandler, errChan chan error) {
	// add logging information
	x.logger.Infof("consuming messages from topic=(%s)", topic)
	// create a subscription
	//subscription, err := x.connection.Subscribe(topic, func(msg *natsclient.Msg) {
	//	//
	//})
}
