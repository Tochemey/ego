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

package eventadapter

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// noopAdapter returns the event unchanged.
type noopAdapter struct{}

func (a *noopAdapter) Adapt(event *anypb.Any, _ uint64) (*anypb.Any, error) {
	return event, nil
}

// timestampToDurationAdapter transforms a Timestamp event into a Duration
// using the seconds field from the timestamp.
type timestampToDurationAdapter struct{}

func (a *timestampToDurationAdapter) Adapt(event *anypb.Any, _ uint64) (*anypb.Any, error) {
	var ts timestamppb.Timestamp
	if err := event.UnmarshalTo(&ts); err != nil {
		// not a Timestamp; pass through
		return event, nil
	}
	return anypb.New(durationpb.New(ts.AsTime().Sub(ts.AsTime()) + ts.AsTime().Sub(ts.AsTime())))
}

// errorAdapter always returns an error.
type errorAdapter struct {
	err error
}

func (a *errorAdapter) Adapt(_ *anypb.Any, _ uint64) (*anypb.Any, error) {
	return nil, a.err
}

// revisionGatedAdapter only transforms events at or above a given revision.
// It replaces a Timestamp event with a Duration of 42 seconds.
type revisionGatedAdapter struct {
	minRevision uint64
}

func (a *revisionGatedAdapter) Adapt(event *anypb.Any, revision uint64) (*anypb.Any, error) {
	if revision < a.minRevision {
		return event, nil
	}
	return anypb.New(durationpb.New(42_000_000_000)) // 42 seconds
}

// addSecondsAdapter adds extra seconds to a Duration event.
type addSecondsAdapter struct {
	extra int64
}

func (a *addSecondsAdapter) Adapt(event *anypb.Any, _ uint64) (*anypb.Any, error) {
	var d durationpb.Duration
	if err := event.UnmarshalTo(&d); err != nil {
		return event, nil
	}
	d.Seconds += a.extra
	return anypb.New(&d)
}

func TestChainNoAdapters(t *testing.T) {
	event, err := anypb.New(timestamppb.Now())
	require.NoError(t, err)

	// nil slice
	result, err := Chain(nil, event, 1)
	require.NoError(t, err)
	assert.Same(t, event, result)

	// empty slice
	result, err = Chain([]EventAdapter{}, event, 1)
	require.NoError(t, err)
	assert.Same(t, event, result)
}

func TestChainSingleAdapterTransforms(t *testing.T) {
	ts := &timestamppb.Timestamp{Seconds: 1000, Nanos: 0}
	event, err := anypb.New(ts)
	require.NoError(t, err)

	adapters := []EventAdapter{&timestampToDurationAdapter{}}
	result, err := Chain(adapters, event, 1)
	require.NoError(t, err)

	// The result should be a Duration (different type URL from the input Timestamp)
	var d durationpb.Duration
	require.NoError(t, result.UnmarshalTo(&d))
	// timestampToDurationAdapter produces a zero duration
	assert.Equal(t, int64(0), d.GetSeconds())
}

func TestChainMultipleAdaptersAppliedInOrder(t *testing.T) {
	// Start with a Duration of 10 seconds
	event, err := anypb.New(durationpb.New(10_000_000_000)) // 10s
	require.NoError(t, err)

	adapters := []EventAdapter{
		&addSecondsAdapter{extra: 5},
		&addSecondsAdapter{extra: 20},
	}

	result, err := Chain(adapters, event, 1)
	require.NoError(t, err)

	var d durationpb.Duration
	require.NoError(t, result.UnmarshalTo(&d))
	// 10 + 5 + 20 = 35 seconds
	assert.Equal(t, int64(35), d.GetSeconds())
}

func TestChainAdapterReturnsError(t *testing.T) {
	event, err := anypb.New(timestamppb.Now())
	require.NoError(t, err)

	expectedErr := errors.New("adapter failure")
	adapters := []EventAdapter{
		&noopAdapter{},
		&errorAdapter{err: expectedErr},
		&addSecondsAdapter{extra: 100}, // should never run
	}

	result, err := Chain(adapters, event, 1)
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, result)
}

func TestChainErrorStopsEarly(t *testing.T) {
	event, err := anypb.New(durationpb.New(10_000_000_000))
	require.NoError(t, err)

	expectedErr := errors.New("mid-chain error")
	adapters := []EventAdapter{
		&addSecondsAdapter{extra: 5},
		&errorAdapter{err: expectedErr},
		&addSecondsAdapter{extra: 100}, // should never run
	}

	result, err := Chain(adapters, event, 1)
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, result)
}

func TestChainNoopAdapter(t *testing.T) {
	original := timestamppb.Now()
	event, err := anypb.New(original)
	require.NoError(t, err)

	adapters := []EventAdapter{&noopAdapter{}}
	result, err := Chain(adapters, event, 1)
	require.NoError(t, err)
	// The pointer should be unchanged since noop returns the same event
	assert.Same(t, event, result)

	// Content should still unmarshal to the same timestamp
	var ts timestamppb.Timestamp
	require.NoError(t, result.UnmarshalTo(&ts))
	assert.True(t, proto.Equal(original, &ts))
}

func TestChainAdapterUsesRevision(t *testing.T) {
	event, err := anypb.New(timestamppb.Now())
	require.NoError(t, err)

	adapters := []EventAdapter{&revisionGatedAdapter{minRevision: 10}}

	// revision below threshold: event passes through unchanged
	result, err := Chain(adapters, event, 5)
	require.NoError(t, err)
	assert.Same(t, event, result)

	// revision at threshold: event is transformed to a Duration
	result, err = Chain(adapters, event, 10)
	require.NoError(t, err)
	var d durationpb.Duration
	require.NoError(t, result.UnmarshalTo(&d))
	assert.Equal(t, int64(42), d.GetSeconds())

	// revision above threshold: event is also transformed
	result, err = Chain(adapters, event, 100)
	require.NoError(t, err)
	require.NoError(t, result.UnmarshalTo(&d))
	assert.Equal(t, int64(42), d.GetSeconds())
}

func TestChainMixedNoopAndTransform(t *testing.T) {
	event, err := anypb.New(durationpb.New(1_000_000_000)) // 1s
	require.NoError(t, err)

	adapters := []EventAdapter{
		&noopAdapter{},
		&addSecondsAdapter{extra: 7},
		&noopAdapter{},
	}

	result, err := Chain(adapters, event, 1)
	require.NoError(t, err)

	var d durationpb.Duration
	require.NoError(t, result.UnmarshalTo(&d))
	assert.Equal(t, int64(8), d.GetSeconds())
}
