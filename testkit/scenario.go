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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// EventSourcedBehavior is the subset of ego.EventSourcedBehavior the scenarios
// exercise, declared here because the testkit cannot import ego (ego's own tests
// import the testkit). Every ego.EventSourcedBehavior satisfies it structurally,
// so a scenario tests the very behavior the engine runs — never a copy written
// for the test. A compile-time assertion in the ego package keeps it that way.
type EventSourcedBehavior interface {
	InitialState() proto.Message
	HandleCommand(ctx context.Context, command proto.Message, priorState proto.Message) (events []proto.Message, err error)
	HandleEvent(ctx context.Context, event proto.Message, priorState proto.Message) (state proto.Message, err error)
}

// DurableStateBehavior is the subset of ego.DurableStateBehavior the scenarios
// exercise, declared here because the testkit cannot import ego (ego's own tests
// import the testkit). Every ego.DurableStateBehavior satisfies it structurally,
// so a scenario tests the very behavior the engine runs — never a copy written
// for the test. A compile-time assertion in the ego package keeps it that way.
type DurableStateBehavior interface {
	InitialState() proto.Message
	HandleCommand(ctx context.Context, command proto.Message, priorVersion uint64, priorState proto.Message) (newState proto.Message, newVersion uint64, err error)
}

// EventSourcedScenario provides a fluent API for testing EventSourcedBehavior
// implementations without starting an engine. It exercises HandleCommand and
// HandleEvent directly.
type EventSourcedScenario struct {
	behavior    EventSourcedBehavior
	priorState  proto.Message
	priorEvents []proto.Message
}

// EventSourcedScenarioResult holds the outcome of processing a command.
type EventSourcedScenarioResult struct {
	events []proto.Message
	state  proto.Message
	// err is the failure of the command under test.
	err error
	// arrangeErr is the failure to build the prior state from the given events. It is
	// kept apart from err so a broken arrangement is never read as a command outcome.
	arrangeErr error
}

// ForEventSourcedBehavior creates a new test scenario for the given behavior.
func ForEventSourcedBehavior(behavior EventSourcedBehavior) *EventSourcedScenario {
	return &EventSourcedScenario{
		behavior: behavior,
	}
}

// Given sets the state the entity is already in when the command arrives. It is
// handed to HandleCommand verbatim, exactly as the engine hands over the state it
// recovered from the journal, so the arrangement does not depend on HandleEvent
// being correct. When Given is omitted, the behavior's InitialState is used.
//
// Given combines with GivenEvents: the state set here is the one those events are
// applied to, mirroring an entity recovered from a snapshot and then replayed.
func (s *EventSourcedScenario) Given(priorState proto.Message) *EventSourcedScenario {
	s.priorState = priorState
	return s
}

// GivenEvents sets the history the entity has already recorded. The events are
// applied in order via HandleEvent to derive the state the command is handled
// against, mirroring how the engine replays a journal, and so exercise HandleEvent
// as part of the scenario. Use Given instead to state the prior state outright and
// keep the arrangement independent of HandleEvent.
//
// An event that HandleEvent rejects fails the scenario as a broken arrangement:
// every assertion reports it as such rather than as a command failure.
func (s *EventSourcedScenario) GivenEvents(events ...proto.Message) *EventSourcedScenario {
	s.priorEvents = events
	return s
}

// When processes the command against the prior state and returns the result.
// The events returned by HandleCommand are applied in order via HandleEvent,
// mirroring how the engine derives the resulting state.
func (s *EventSourcedScenario) When(command proto.Message) *EventSourcedScenarioResult {
	ctx := context.Background()

	state := s.priorState
	if state == nil {
		state = s.behavior.InitialState()
	}

	for _, event := range s.priorEvents {
		replayedState, err := s.behavior.HandleEvent(ctx, event, state)
		if err != nil {
			return &EventSourcedScenarioResult{arrangeErr: fmt.Errorf("given events could not be applied: %w", err)}
		}

		state = replayedState
	}

	events, err := s.behavior.HandleCommand(ctx, command, state)
	if err != nil {
		return &EventSourcedScenarioResult{err: err}
	}

	for _, event := range events {
		state, err = s.behavior.HandleEvent(ctx, event, state)
		if err != nil {
			return &EventSourcedScenarioResult{err: err}
		}
	}

	return &EventSourcedScenarioResult{events: events, state: state}
}

// requireArranged fails the test when the given events could not be applied, so a
// broken arrangement is never reported as an outcome of the command under test.
func (r *EventSourcedScenarioResult) requireArranged(t testing.TB) {
	t.Helper()
	require.NoError(t, r.arrangeErr, "scenario arrangement failed")
}

// ThenEvents asserts that the command produced exactly these events
// (using proto.Equal for comparison). Returns itself for chaining.
func (r *EventSourcedScenarioResult) ThenEvents(t testing.TB, expected ...proto.Message) *EventSourcedScenarioResult {
	t.Helper()
	r.requireArranged(t)
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
	r.requireArranged(t)
	require.NoError(t, r.err, "command processing returned an error")
	assert.True(t, proto.Equal(expected, r.state),
		"state mismatch: expected %v, got %v", expected, r.state)
	return r
}

// ThenError asserts the command returned an error containing the given substring.
func (r *EventSourcedScenarioResult) ThenError(t testing.TB, errSubstring string) *EventSourcedScenarioResult {
	t.Helper()
	r.requireArranged(t)
	require.Error(t, r.err, "expected an error but got none")
	assert.Contains(t, r.err.Error(), errSubstring)
	return r
}

// ThenNoEvents asserts the command produced no events (no-op).
func (r *EventSourcedScenarioResult) ThenNoEvents(t testing.TB) *EventSourcedScenarioResult {
	t.Helper()
	r.requireArranged(t)
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
