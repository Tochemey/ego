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

package ego

import (
	"testing"
	"time"

	goset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v3/discovery/kubernetes"
	"github.com/tochemey/goakt/v3/log"

	"github.com/tochemey/ego/v3/projection"
)

func TestOptions(t *testing.T) {
	// use the default logger of GoAkt
	logger := log.DefaultLogger
	// create a discovery provider
	discoveryProvider := kubernetes.NewDiscovery(&kubernetes.Config{})

	testCases := []struct {
		name     string
		option   Option
		expected func() *Engine
	}{
		{
			name:   "WithCluster",
			option: WithCluster(discoveryProvider, 30, 3, "localhost", 1334, 1335, 1336),
			expected: func() *Engine {
				expected := &Engine{
					discoveryProvider:  discoveryProvider,
					minimumPeersQuorum: 3,
					bindAddr:           "localhost",
					discoveryPort:      1335,
					peersPort:          1336,
					remotingPort:       1334,
					partitionsCount:    30,
				}
				expected.clusterEnabled.Store(true)
				return expected
			},
		},
		{
			name:   "WithLogger",
			option: WithLogger(logger),
			expected: func() *Engine {
				return &Engine{logger: logger}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			engine := new(Engine)
			tc.option.Apply(engine)
			assert.Equal(t, tc.expected(), engine)
		})
	}
}

func TestOptionWithProjection(t *testing.T) {
	handler := projection.NewDiscardHandler()
	engine := new(Engine)
	opt := WithProjection(handler, 500, time.Time{}, time.Time{}, time.Second, nil)
	opt.Apply(engine)
	require.NotEmpty(t, engine.projectionExtension)
	require.NotNil(t, engine.projectionExtension.Recovery())
}

func TestOptionWithProjectionOptions(t *testing.T) {
	handler := projection.NewDiscardHandler()
	recovery := projection.NewRecovery(projection.WithRetries(10))
	engine := new(Engine)
	opt := WithProjectionOptions(&projection.Options{
		Handler:      handler,
		BufferSize:   500,
		StartOffset:  time.Time{},
		ResetOffset:  time.Time{},
		PullInterval: time.Second,
		Recovery:     recovery,
	})
	opt.Apply(engine)
	require.NotEmpty(t, engine.projectionExtension)
	require.Same(t, recovery, engine.projectionExtension.Recovery())
}

func TestOptionWithRoles(t *testing.T) {
	engine := &Engine{roles: goset.NewSet[string]()}
	opt := WithRoles("role1", "role2")
	opt.Apply(engine)
	assert.ElementsMatch(t, []string{"role1", "role2"}, engine.roles.ToSlice())
}
