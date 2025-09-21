/*
 * MIT License
 *
 * Copyright (c) 2023-2025 Tochemey
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
}
