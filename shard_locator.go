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

import goakt "github.com/tochemey/goakt/v4/actor"

// shardLocator resolves the journal shard of an actor's records when the
// first record is written and keeps it for the rest of the actor's life.
//
// The shard of an actor is the cluster partition of its name. Actor system
// releases up to v4.6.1 scheduled PostStart before publishing the actor to
// the cluster registry, so a lookup made in PostStart found no record and
// yielded shard zero for every actor. The first record is written on a
// command instead, after the spawn that publishes the actor has returned to
// its caller, so a lookup made then finds the record in place whatever the
// actor system release.
type shardLocator struct {
	resolved bool
	shard    uint64
}

// locate returns the shard of the actor named name on the given actor system.
// The first call looks it up; later calls return the value kept.
func (locator *shardLocator) locate(system goakt.ActorSystem, name string) uint64 {
	if !locator.resolved {
		locator.shard = system.Partition(name)
		locator.resolved = true
	}

	return locator.shard
}
