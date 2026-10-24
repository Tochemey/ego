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

package persistence

import "hash/fnv"

// NumberOfShards is the number of journal shards. Every record of a persistence
// ID is stored on the shard ShardFor returns for it, and projections read the
// journal one shard at a time.
//
// The value is part of the journal layout and never changes: a different count
// would move persistence IDs to other shards, and records of one persistence ID
// spread over two shards can reach a projection out of order.
const NumberOfShards uint64 = 1024

// ShardFor returns the journal shard of the given persistence ID, a number in
// [0, NumberOfShards). It is the FNV-1a 64-bit hash of the persistence ID
// modulo NumberOfShards, so it depends on nothing but the persistence ID: not on
// the cluster, the node or the partition count.
//
// Events stores, durable state stores and migration tools that need the shard
// of a persistence ID must use this function. Like NumberOfShards, it never
// changes.
func ShardFor(persistenceID string) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(persistenceID))
	return hasher.Sum64() % NumberOfShards
}
