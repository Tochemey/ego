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
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsclient "github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/tochemey/goakt/log"
	"go.uber.org/atomic"
)

var (
	// ErrStreamServerNotStarted is returned when the stream server is not started
	ErrStreamServerNotStarted = errors.New("events stream server not started")
	// ErrStreamServerStartFailure is returned when the stream server fails to start
	ErrStreamServerStartFailure = errors.New("events stream server failed to start")
	// ErrStreamServerAlreadyStarted is returned when the stream server is already started
	ErrStreamServerAlreadyStarted = errors.New("events stream server already started")
	// ErrStreamPublisherNotConnected is returned when the stream client is not connected
	ErrStreamPublisherNotConnected = errors.New("events stream publisher is not connected")
)

// EventsStream defines the events stream engine
type EventsStream struct {
	// underlying NATS JetStream server
	server *natsserver.Server
	// underlying NATS client connection
	client *natsclient.Conn
	logger log.Logger

	started      *atomic.Bool
	startTimeout time.Duration

	publisher *natsclient.Conn
}

// NewEventsStream creates an instance of EventsStream
func NewEventsStream() (*EventsStream, error) {
	// create an instance of the stream server
	// TODO: add optional params to overwrite this default settings
	streamServer := &EventsStream{
		logger:       log.DefaultLogger,
		started:      atomic.NewBool(false),
		startTimeout: 5 * time.Second,
	}

	// create the instance of the NATS JetStream server
	// TODO: enhance the configuration when needed
	server, err := natsserver.NewServer(&natsserver.Options{
		DontListen: true,
		JetStream:  true,
		Logtime:    true,
		NoLog:      false,
	})
	// return an error in case there is one
	if err != nil {
		return nil, err
	}

	// create the NATS logger
	l := newNLogger(streamServer.logger)
	// set the debug flag
	debugFlag := streamServer.logger.LogLevel() == log.DebugLevel
	// set the server logger
	server.SetLogger(l, debugFlag, true)
	// configure the logger
	server.ConfigureLogger()
	// set the stream server underlying server
	streamServer.server = server

	// successful
	return streamServer, nil
}

// Start starts the stream server
// nolint
func (x *EventsStream) Start(ctx context.Context) error {
	// add a logging information
	x.logger.Info("starting the events stream server...")
	// check whether the server is already started
	if x.started.Load() {
		return ErrStreamServerAlreadyStarted
	}

	// start the NATS JetStream server
	x.server.Start()
	// wait for the server to be ready
	if !x.server.ReadyForConnections(x.startTimeout) {
		x.logger.Error("stream server failed to start")
		return ErrStreamServerStartFailure
	}

	// set the started
	x.started.Store(true)
	// add a logging information
	x.logger.Info("events stream server started successfully")
	return nil
}

// Stop stops the stream server
func (x *EventsStream) Stop() error {
	// add a logging information
	x.logger.Info("stopping the events stream server...")
	// check whether the server is already started
	if !x.started.Load() {
		return ErrStreamServerNotStarted
	}

	// shutdown the underlying NATS JetStream server
	x.server.Shutdown()
	// set the started to false
	x.started.Store(false)
	return nil
}

// Publisher returns a publisher instance
func (x *EventsStream) Publisher() (*Publisher, error) {
	// create a client connection
	conn, err := natsclient.Connect("", natsclient.InProcessServer(x.server))
	// handle the error
	if err != nil {
		x.logger.Error(errors.Wrap(err, "failed to create an events stream client connection"))
		return nil, err
	}
	// create the client instance
	return &Publisher{
		connection: conn,
		logger:     x.logger,
		connected:  atomic.NewBool(true),
	}, nil
}
