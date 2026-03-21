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

	"github.com/stretchr/testify/require"
)

func TestSpawnOption(t *testing.T) {
	t.Run("WithPassivateAfter with options", func(t *testing.T) {
		config := newSpawnConfig(WithPassivateAfter(time.Second))
		require.EqualValues(t, time.Second, config.passivateAfter)
	})
	t.Run("WithPassivateAfter with Apply", func(t *testing.T) {
		config := &spawnConfig{}
		second := time.Second
		option := WithPassivateAfter(second)
		option.Apply(config)
		require.Equal(t, &spawnConfig{passivateAfter: second}, config)
	})
	t.Run("WithRelocation", func(t *testing.T) {
		config := &spawnConfig{}
		option := WithRelocation(true)
		option.Apply(config)
		require.Equal(t, &spawnConfig{toRelocate: true}, config)
	})
	t.Run("WithSupervisorDirective", func(t *testing.T) {
		config := &spawnConfig{}
		option := WithSupervisorDirective(StopDirective)
		option.Apply(config)
		require.Equal(t, &spawnConfig{supervisorDirective: StopDirective}, config)
	})
	t.Run("WithEntitiesPlacement", func(t *testing.T) {
		config := &spawnConfig{}
		option := WithPlacement(LeastLoad)
		option.Apply(config)
		require.Equal(t, &spawnConfig{entitiesPlacement: LeastLoad}, config)
	})
	t.Run("WithSnapshotInterval", func(t *testing.T) {
		config := &spawnConfig{}
		option := WithSnapshotInterval(10)
		option.Apply(config)
		require.EqualValues(t, 10, config.snapshotInterval)
	})
	t.Run("WithSnapshotInterval zero is default", func(t *testing.T) {
		config := newSpawnConfig()
		require.EqualValues(t, 0, config.snapshotInterval)
	})
	t.Run("WithRetentionPolicy", func(t *testing.T) {
		policy := RetentionPolicy{
			DeleteEventsOnSnapshot:    true,
			DeleteSnapshotsOnSnapshot: true,
			EventsRetentionCount:      100,
		}
		config := &spawnConfig{}
		option := WithRetentionPolicy(policy)
		option.Apply(config)
		require.NotNil(t, config.retentionPolicy)
		require.True(t, config.retentionPolicy.DeleteEventsOnSnapshot)
		require.True(t, config.retentionPolicy.DeleteSnapshotsOnSnapshot)
		require.EqualValues(t, 100, config.retentionPolicy.EventsRetentionCount)
	})
	t.Run("default config has no retention policy", func(t *testing.T) {
		config := newSpawnConfig()
		require.Nil(t, config.retentionPolicy)
	})
	t.Run("default config has RestartDirective", func(t *testing.T) {
		config := newSpawnConfig()
		require.Equal(t, RestartDirective, config.supervisorDirective)
	})
	t.Run("default config has RoundRobin placement", func(t *testing.T) {
		config := newSpawnConfig()
		require.Equal(t, RoundRobin, config.entitiesPlacement)
	})
}
