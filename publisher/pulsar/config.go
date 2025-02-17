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
	"fmt"
	"strings"
	"time"

	"github.com/tochemey/gopack/validation"
)

// Config is a set of base config values required for connecting to Pulsar
type Config struct {
	// URL defines the pulsar server in the format pulsar://host:port
	URL string
	// EventsTopic is the topic for publishing events.
	EventsTopic string
	// StateTopic is the topic for publishing state changes.
	StateTopic string
	// ConnectionTimeout specifies the timeout for establishing a connection.
	ConnectionTimeout time.Duration
	// KeepAlive specifies the keep-alive period for the connection.
	KeepAlive time.Duration
}

var _ validation.Validator = (*Config)(nil)

func (x Config) Validate() error {
	return validation.New(validation.FailFast()).
		AddValidator(validation.NewEmptyStringValidator("URL", x.URL)).
		AddValidator(NewURLValidator(x.URL)).
		AddValidator(validation.NewEmptyStringValidator("EventsTopic", x.EventsTopic)).
		AddValidator(validation.NewEmptyStringValidator("StateTopic", x.StateTopic)).
		Validate()
}

type URLValidator struct {
	url string
}

// enforce compilation error
var _ validation.Validator = (*URLValidator)(nil)

func NewURLValidator(url string) validation.Validator {
	return &URLValidator{url: url}
}

// Validate execute the validation code
func (x *URLValidator) Validate() error {
	if !strings.HasPrefix(x.url, "pulsar") {
		return fmt.Errorf("invalid nats server address: %s", x.url)
	}

	hostAndPort := strings.SplitN(x.url, "pulsar://", 2)[1]
	return validation.NewTCPAddressValidator(hostAndPort).Validate()
}
