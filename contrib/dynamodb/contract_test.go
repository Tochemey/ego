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

package dynamodb

import (
	"testing"

	"github.com/tochemey/ego/v5/offsetstore"
	"github.com/tochemey/ego/v5/offsetstore/offsetstoretest"
	"github.com/tochemey/ego/v5/persistence"
	"github.com/tochemey/ego/v5/persistence/persistencetest"
)

func TestEventsStoreContract(t *testing.T) {
	persistencetest.TestEventsStore(t, func(t *testing.T) persistence.EventsStore {
		return NewEventsStore(createEventsTable(t), testClient)
	})
}

func TestSnapshotStoreContract(t *testing.T) {
	persistencetest.TestSnapshotStore(t, func(t *testing.T) persistence.SnapshotStore {
		return NewSnapshotStore(createSnapshotsTable(t), testClient)
	})
}

func TestStateStoreContract(t *testing.T) {
	persistencetest.TestStateStore(t, func(t *testing.T) persistence.StateStore {
		return NewDurableStore(createStatesTable(t), testClient)
	})
}

func TestOffsetStoreContract(t *testing.T) {
	offsetstoretest.TestOffsetStore(t, func(t *testing.T) offsetstore.OffsetStore {
		return NewOffsetStore(createOffsetsTable(t), testClient)
	})
}
