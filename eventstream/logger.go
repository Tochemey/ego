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
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/tochemey/goakt/log"
)

// nLogger defines the NATS JetStream logger
type nLogger struct {
	logger log.Logger
}

// newNLogger creates an instance of nLogger
func newNLogger(logger log.Logger) *nLogger {
	return &nLogger{logger: logger}
}

// Noticef implementation
func (n nLogger) Noticef(format string, v ...interface{}) {
	n.logger.Infof(format, v...)
}

// Warnf implementation
func (n nLogger) Warnf(format string, v ...interface{}) {
	n.logger.Warnf(format, v...)
}

// Fatalf implementation
func (n nLogger) Fatalf(format string, v ...interface{}) {
	n.logger.Fatalf(format, v...)
}

// Errorf implementation
func (n nLogger) Errorf(format string, v ...interface{}) {
	n.logger.Errorf(format, v...)
}

// Debugf implementation
func (n nLogger) Debugf(format string, v ...interface{}) {
	n.logger.Debugf(format, v...)
}

// Tracef implementation
func (n nLogger) Tracef(format string, v ...interface{}) {
	// TODO: maybe use info or implements trace
	n.logger.Warnf(format, v...)
}

// enforce the implementation of the NATS logger interface
var _ natsserver.Logger = (*nLogger)(nil)
