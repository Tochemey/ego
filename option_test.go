package ego

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tochemey/goakt/discovery"
	"github.com/tochemey/goakt/discovery/kubernetes"
	"github.com/tochemey/goakt/log"
	"github.com/tochemey/goakt/telemetry"
	"go.uber.org/atomic"
)

func TestOptions(t *testing.T) {
	// use the default logger of GoAkt
	logger := log.DefaultLogger
	// create a discovery provider
	discoveryProvider := kubernetes.NewDiscovery()
	config := discovery.NewConfig()
	tel := telemetry.New()

	testCases := []struct {
		name     string
		option   Option
		expected Engine
	}{
		{
			name:   "WithCluster",
			option: WithCluster(discoveryProvider, config, 30),
			expected: Engine{
				discoveryProvider: discoveryProvider,
				discoveryConfig:   config,
				partitionsCount:   30,
				enableCluster:     atomic.NewBool(true),
			},
		},
		{
			name:     "WithLogger",
			option:   WithLogger(logger),
			expected: Engine{logger: logger},
		},
		{
			name:     "WithTelemetry",
			option:   WithTelemetry(tel),
			expected: Engine{telemetry: tel},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e Engine
			tc.option.Apply(&e)
			assert.Equal(t, tc.expected, e)
		})
	}
}
