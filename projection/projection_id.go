/*
 * MIT License
 *
 * Copyright (c) 2002-2023 Tochemey
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

package projection

type id struct {
	// the projection projectionName. This must be unique within an actor system
	// the same name is used in all projection IDs for a given a projection
	projectionName string
	// specifies the shard number.
	shardNumber uint64
}

// newID creates a new instance of Projection
func newID(projectionName string, shardNumber uint64) *id {
	return &id{
		projectionName: projectionName,
		shardNumber:    shardNumber,
	}
}

// ProjectionName returns the projection name
func (x id) ProjectionName() string {
	return x.projectionName
}

// ShardNumber returns the shard number
func (x id) ShardNumber() uint64 {
	return x.shardNumber
}
