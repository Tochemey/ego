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

package cassandra

import (
	"testing"

	"github.com/tochemey/ego/v5/offsetstore"
	"github.com/tochemey/ego/v5/offsetstore/offsetstoretest"
	"github.com/tochemey/ego/v5/persistence"
	"github.com/tochemey/ego/v5/persistence/persistencetest"
)

func TestSnapshotStoreContract(t *testing.T) {
	persistencetest.TestSnapshotStore(t, func(t *testing.T) persistence.SnapshotStore {
		session := newSession(t, testKeyspace)
		truncate(t, session, "snapshots_store")
		return NewSnapshotStore(session)
	})
}

func TestStateStoreContract(t *testing.T) {
	persistencetest.TestStateStore(t, func(t *testing.T) persistence.StateStore {
		session := newSession(t, testKeyspace)
		truncate(t, session, "states_store")
		return NewDurableStore(session)
	})
}

func TestOffsetStoreContract(t *testing.T) {
	offsetstoretest.TestOffsetStore(t, func(t *testing.T) offsetstore.OffsetStore {
		session := newSession(t, testKeyspace)
		truncate(t, session, "offsets_store", "offsets_by_entity", "offsets_start")
		return NewOffsetStore(session)
	})
}
