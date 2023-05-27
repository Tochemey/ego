package ego

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tochemey/goakt/discovery/kubernetes"
	"github.com/tochemey/goakt/log"
	"github.com/tochemey/goakt/pkg/telemetry"
)

func TestOptions(t *testing.T) {
	// use the default logger of GoAkt
	logger := log.DefaultLogger
	// create a discovery provider
	disco := kubernetes.New(logger)
	tel := telemetry.New()

	testCases := []struct {
		name     string
		option   Option
		expected Ego
	}{
		{
			name:     "WithCluster",
			option:   WithCluster(disco, "localhost", 3333),
			expected: Ego{discoveryMode: disco, remotingHost: "localhost", remotingPort: 3333, enableCluster: true},
		},
		{
			name:     "WithLogger",
			option:   WithLogger(logger),
			expected: Ego{logger: logger},
		},
		{
			name:     "WithTelemetry",
			option:   WithTelemetry(tel),
			expected: Ego{telemetry: tel},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e Ego
			tc.option.Apply(&e)
			assert.Equal(t, tc.expected, e)
		})
	}
}
