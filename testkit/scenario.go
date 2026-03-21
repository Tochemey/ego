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

package testkit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// EventSourcedBehavior mirrors ego.EventSourcedBehavior for use in the testkit
// without creating an import cycle. Any ego.EventSourcedBehavior satisfies this interface.
type EventSourcedBehavior interface {
	InitialState() proto.Message
	HandleCommand(ctx context.Context, command proto.Message, priorState proto.Message) (events []proto.Message, err error)
	HandleEvent(ctx context.Context, event proto.Message, priorState proto.Message) (state proto.Message, err error)
}

// DurableStateBehavior mirrors ego.DurableStateBehavior for use in the testkit
// without creating an import cycle. Any ego.DurableStateBehavior satisfies this interface.
type DurableStateBehavior interface {
	InitialState() proto.Message
	HandleCommand(ctx context.Context, command proto.Message, priorVersion uint64, priorState proto.Message) (newState proto.Message, newVersion uint64, err error)
}

// EventSourcedScenario provides a fluent API for testing EventSourcedBehavior
// implementations without starting an engine. It exercises HandleCommand and
// HandleEvent directly.
type EventSourcedScenario struct {
	behavior    EventSourcedBehavior
	priorEvents []proto.Message
}

// EventSourcedScenarioResult holds the outcome of processing a command.
type EventSourcedScenarioResult struct {
	events []proto.Message
	state  proto.Message
	err    error
}

// ForEventSourcedBehavior creates a new test scenario for the given behavior.
func ForEventSourcedBehavior(behavior EventSourcedBehavior) *EventSourcedScenario {
	return &EventSourcedScenario{
		behavior: behavior,
	}
}

// Given sets up prior events that are applied (via HandleEvent) to build
// the initial state before the command is sent. This simulates an entity
// that has already processed these events.
func (s *EventSourcedScenario) Given(events ...proto.Message) *EventSourcedScenario {
	s.priorEvents = events
	return s
}

// When sets the command to process and returns the result.
func (s *EventSourcedScenario) When(command proto.Message) *EventSourcedScenarioResult {
	ctx := context.Background()

	// build up state from prior events
	state := s.behavior.InitialState()
	for _, event := range s.priorEvents {
		var err error
		state, err = s.behavior.HandleEvent(ctx, event, state)
		if err != nil {
			return &EventSourcedScenarioResult{err: err}
		}
	}

	// process the command
	events, err := s.behavior.HandleCommand(ctx, command, state)
	if err != nil {
		return &EventSourcedScenarioResult{err: err}
	}

	// apply produced events to get final state
	for _, event := range events {
		state, err = s.behavior.HandleEvent(ctx, event, state)
		if err != nil {
			return &EventSourcedScenarioResult{err: err}
		}
	}

	return &EventSourcedScenarioResult{events: events, state: state}
}

// ThenEvents asserts that the command produced exactly these events
// (using proto.Equal for comparison). Returns itself for chaining.
func (r *EventSourcedScenarioResult) ThenEvents(t testing.TB, expected ...proto.Message) *EventSourcedScenarioResult {
	t.Helper()
	require.NoError(t, r.err, "command processing returned an error")
	require.Len(t, r.events, len(expected), "unexpected number of events")
	for i, exp := range expected {
		assert.True(t, proto.Equal(exp, r.events[i]),
			"event at index %d: expected %v, got %v", i, exp, r.events[i])
	}
	return r
}

// ThenState asserts the final state after all produced events are applied.
func (r *EventSourcedScenarioResult) ThenState(t testing.TB, expected proto.Message) *EventSourcedScenarioResult {
	t.Helper()
	require.NoError(t, r.err, "command processing returned an error")
	assert.True(t, proto.Equal(expected, r.state),
		"state mismatch: expected %v, got %v", expected, r.state)
	return r
}

// ThenError asserts the command returned an error containing the given substring.
func (r *EventSourcedScenarioResult) ThenError(t testing.TB, errSubstring string) *EventSourcedScenarioResult {
	t.Helper()
	require.Error(t, r.err, "expected an error but got none")
	assert.Contains(t, r.err.Error(), errSubstring)
	return r
}

// ThenNoEvents asserts the command produced no events (no-op).
func (r *EventSourcedScenarioResult) ThenNoEvents(t testing.TB) *EventSourcedScenarioResult {
	t.Helper()
	require.NoError(t, r.err, "command processing returned an error")
	assert.Empty(t, r.events, "expected no events but got %d", len(r.events))
	return r
}

// DurableStateScenario provides a fluent API for testing DurableStateBehavior
// implementations without starting an engine.
type DurableStateScenario struct {
	behavior     DurableStateBehavior
	priorState   proto.Message
	priorVersion uint64
}

// DurableStateScenarioResult holds the outcome of processing a command.
type DurableStateScenarioResult struct {
	state   proto.Message
	version uint64
	err     error
}

// ForDurableStateBehavior creates a new test scenario for the given behavior.
func ForDurableStateBehavior(behavior DurableStateBehavior) *DurableStateScenario {
	return &DurableStateScenario{
		behavior: behavior,
	}
}

// Given sets up a prior state and version before the command is sent.
func (s *DurableStateScenario) Given(state proto.Message, version uint64) *DurableStateScenario {
	s.priorState = state
	s.priorVersion = version
	return s
}

// When sets the command to process and returns the result.
func (s *DurableStateScenario) When(command proto.Message) *DurableStateScenarioResult {
	ctx := context.Background()

	priorState := s.priorState
	if priorState == nil {
		priorState = s.behavior.InitialState()
	}

	newState, newVersion, err := s.behavior.HandleCommand(ctx, command, s.priorVersion, priorState)
	if err != nil {
		return &DurableStateScenarioResult{err: err}
	}

	return &DurableStateScenarioResult{state: newState, version: newVersion}
}

// ThenState asserts the resulting state matches expected.
func (r *DurableStateScenarioResult) ThenState(t testing.TB, expected proto.Message) *DurableStateScenarioResult {
	t.Helper()
	require.NoError(t, r.err, "command processing returned an error")
	assert.True(t, proto.Equal(expected, r.state),
		"state mismatch: expected %v, got %v", expected, r.state)
	return r
}

// ThenVersion asserts the resulting version matches expected.
func (r *DurableStateScenarioResult) ThenVersion(t testing.TB, expected uint64) *DurableStateScenarioResult {
	t.Helper()
	require.NoError(t, r.err, "command processing returned an error")
	assert.Equal(t, expected, r.version)
	return r
}

// ThenError asserts the command returned an error containing the given substring.
func (r *DurableStateScenarioResult) ThenError(t testing.TB, errSubstring string) *DurableStateScenarioResult {
	t.Helper()
	require.Error(t, r.err, "expected an error but got none")
	assert.Contains(t, r.err.Error(), errSubstring)
	return r
}
