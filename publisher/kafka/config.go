// MIT License
//
// Copyright (c) 2022-2026 Arsene Tochemey Gandote
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package kafka

import (
	"crypto/tls"
	"time"

	"github.com/IBM/sarama"
	"github.com/tochemey/goakt/v3/log"
)

// Config is a set of base config values required for connecting to Kafka
type Config struct {
	// Brokers is a list of Kafka broker addresses.
	Brokers []string
	// EnableTLS indicates whether TLS should be enabled for the connection.
	EnableTLS bool
	// APIKey is the API key for SASL authentication.
	APIKey string
	// APISecret is the API secret for SASL authentication.
	APISecret string
	// KeepAlive specifies the keep-alive period for the connection.
	KeepAlive time.Duration
	// TLS is the TLS configuration for the connection.
	TLS *tls.Config
	// MaxRetries is the maximum number of retries for sending a message.
	MaxRetries int
	// RetryInterval is the interval between retries.
	RetryInterval time.Duration
	// EventsTopic is the topic for publishing events.
	EventsTopic string
	// StateTopic is the topic for publishing state changes.
	StateTopic string
	// Logger is the logger for the publisher.
	Logger log.Logger
}

func toSaramaConfig(config *Config) *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V0_11_0_2
	saramaConfig.ClientID = "ego-kafka-publisher"

	if config.EnableTLS {
		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = config.TLS
		saramaConfig.Net.TLS.Config.ServerName = config.Brokers[0]
	}

	if config.APIKey != "" && config.APISecret != "" {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = config.APIKey
		saramaConfig.Net.SASL.Password = config.APISecret
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	saramaConfig.Net.KeepAlive = config.KeepAlive
	// producer settings
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Net.MaxOpenRequests = 1
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = config.MaxRetries
	saramaConfig.Producer.Retry.Backoff = config.RetryInterval
	saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner

	return saramaConfig
}
