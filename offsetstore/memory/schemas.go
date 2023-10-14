/*
 * Copyright (c) 2022-2023 Tochemey
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

package memory

import "github.com/hashicorp/go-memdb"

// offsetRow represent the offset entry in the offset store
type offsetRow struct {
	// Ordering basically used as PK
	Ordering string
	// ProjectionName is the projection name
	ProjectionName string
	// Shard Number
	ShardNumber uint64
	// CurrentOffset is the current offset
	CurrentOffset int64
	// Specifies the last update time
	LastUpdated int64
}

const (
	offsetTableName     = "offsets"
	offsetPK            = "id"
	currentOffsetIndex  = "currentOffset"
	projectionNameIndex = "projectionName"
	rowIndex            = "rowIndex"
	shardNumberIndex    = "shardNumber"
)

var (
	// offsetSchema defines the offset schema
	offsetSchema = &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			offsetTableName: {
				Name: offsetTableName,
				Indexes: map[string]*memdb.IndexSchema{
					offsetPK: {
						Name:         offsetPK,
						AllowMissing: false,
						Unique:       true,
						Indexer: &memdb.StringFieldIndex{
							Field:     "Ordering",
							Lowercase: false,
						},
					},
					shardNumberIndex: {
						Name:         shardNumberIndex,
						AllowMissing: false,
						Unique:       false,
						Indexer: &memdb.UintFieldIndex{
							Field: "ShardNumber",
						},
					},
					currentOffsetIndex: {
						Name:         currentOffsetIndex,
						AllowMissing: false,
						Unique:       false,
						Indexer: &memdb.IntFieldIndex{
							Field: "CurrentOffset",
						},
					},
					projectionNameIndex: {
						Name:         projectionNameIndex,
						AllowMissing: false,
						Unique:       false,
						Indexer: &memdb.StringFieldIndex{
							Field: "ProjectionName",
						},
					},
					rowIndex: {
						Name:         rowIndex,
						AllowMissing: false,
						Unique:       true,
						Indexer: &memdb.CompoundIndex{
							Indexes: []memdb.Indexer{
								&memdb.StringFieldIndex{
									Field:     "ProjectionName",
									Lowercase: false,
								},
								&memdb.UintFieldIndex{
									Field: "ShardNumber",
								},
							},
							AllowMissing: false,
						},
					},
				},
			},
		},
	}
)
