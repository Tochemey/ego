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

import (
	"github.com/hashicorp/go-memdb"
)

// journal represents the journal entry
// This matches the RDBMS counter-part.
type journal struct {
	// Ordering basically used as PK
	Ordering string
	// PersistenceID is the persistence ID
	PersistenceID string
	// SequenceNumber
	SequenceNumber uint64
	// Specifies whether the journal is deleted
	IsDeleted bool
	// Specifies the event byte array
	EventPayload []byte
	// Specifies the event manifest
	EventManifest string
	// Specifies the state payload
	StatePayload []byte
	// Specifies the state manifest
	StateManifest string
	// Specifies time the record has been persisted
	Timestamp int64
	// Specifies the shard number
	ShardNumber uint64
}

const (
	journalTableName    = "event_store"
	journalPK           = "id"
	isDeletedIndex      = "deletion"
	persistenceIDIndex  = "persistenceId"
	sequenceNumberIndex = "sequenceNumber"
	shardNumberIndex    = "shardNumber"
	timestampIndex      = "timestamp"
)

var (
	// journalSchema defines the journal schema
	journalSchema = &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			journalTableName: {
				Name: journalTableName,
				Indexes: map[string]*memdb.IndexSchema{
					journalPK: {
						Name:         journalPK,
						AllowMissing: false,
						Unique:       true,
						Indexer: &memdb.StringFieldIndex{
							Field:     "Ordering",
							Lowercase: false,
						},
					},
					isDeletedIndex: {
						Name:         isDeletedIndex,
						AllowMissing: false,
						Unique:       false,
						Indexer: &memdb.StringFieldIndex{
							Field:     "IsDeleted",
							Lowercase: false,
						},
					},
					persistenceIDIndex: {
						Name:         persistenceIDIndex,
						AllowMissing: false,
						Unique:       false,
						Indexer: &memdb.StringFieldIndex{
							Field:     "PersistenceID",
							Lowercase: false,
						},
					},
					sequenceNumberIndex: {
						Name:         sequenceNumberIndex,
						AllowMissing: false,
						Unique:       false,
						Indexer: &memdb.UintFieldIndex{
							Field: "SequenceNumber",
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
					timestampIndex: {
						Name:         timestampIndex,
						AllowMissing: false,
						Unique:       false,
						Indexer: &memdb.IntFieldIndex{
							Field: "Timestamp",
						},
					},
				},
			},
		},
	}
)
