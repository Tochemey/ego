/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
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

package postgres

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v3/egopb"
)

// row represents the events store row
type row struct {
	PersistenceID  string
	SequenceNumber uint64
	IsDeleted      bool
	EventPayload   []byte
	EventManifest  string
	StatePayload   []byte
	StateManifest  string
	Timestamp      int64
	ShardNumber    uint64
}

// ToEvent convert row to event
func (x row) ToEvent() (*egopb.Event, error) {
	// unmarshal the event and the state
	evt, err := toProto(x.EventManifest, x.EventPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal the journal event: %w", err)
	}
	state, err := toProto(x.StateManifest, x.StatePayload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal the journal state: %w", err)
	}

	return &egopb.Event{
		PersistenceId:  x.PersistenceID,
		SequenceNumber: x.SequenceNumber,
		IsDeleted:      x.IsDeleted,
		Event:          evt,
		ResultingState: state,
		Timestamp:      x.Timestamp,
		Shard:          x.ShardNumber,
	}, nil
}

// rows defines the list of row
type rows []*row

// ToEvents converts rows to events
func (x rows) ToEvents() ([]*egopb.Event, error) {
	// create the list of events
	events := make([]*egopb.Event, 0, len(x))
	// iterate the rows
	for _, row := range x {
		// unmarshal the event and the state
		evt, err := toProto(row.EventManifest, row.EventPayload)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal the journal event: %w", err)
		}
		state, err := toProto(row.StateManifest, row.StatePayload)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal the journal state: %w", err)
		}
		// create the event and add it to the list of events
		events = append(events, &egopb.Event{
			PersistenceId:  row.PersistenceID,
			SequenceNumber: row.SequenceNumber,
			IsDeleted:      row.IsDeleted,
			Event:          evt,
			ResultingState: state,
			Timestamp:      row.Timestamp,
			Shard:          row.ShardNumber,
		})
	}

	return events, nil
}

// toProto converts a byte array given its manifest into a valid proto message
func toProto(manifest string, bytea []byte) (*anypb.Any, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(manifest))
	if err != nil {
		return nil, err
	}

	pm := mt.New().Interface()
	err = proto.Unmarshal(bytea, pm)
	if err != nil {
		return nil, err
	}

	if cast, ok := pm.(*anypb.Any); ok {
		return cast, nil
	}
	return nil, fmt.Errorf("failed to unpack message=%s", manifest)
}
