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

package migration

import "github.com/tochemey/goakt/v4/log"

const (
	// DefaultPageSize is how many persistence IDs, or events of one entity, a
	// migration reads at once unless WithPageSize says otherwise.
	DefaultPageSize uint64 = 500
	// DefaultParallelism is how many entities a migration processes at once
	// unless WithParallelism says otherwise.
	DefaultParallelism = 10
)

// settings holds the configuration every migration of this package shares.
type settings struct {
	// pageSize is how many persistence IDs, or events of one entity, are read at once.
	pageSize uint64
	// logger receives the migration's progress.
	logger log.Logger
	// parallelism is how many entities are migrated at once.
	parallelism int
}

// defaultSettings returns the settings a migration starts from.
func defaultSettings() settings {
	return settings{
		pageSize:    DefaultPageSize,
		logger:      log.DefaultLogger,
		parallelism: DefaultParallelism,
	}
}

// Option configures a migration: the Migrator, the JournalMigrator or the
// SnapshotMigrator.
type Option interface {
	apply(s *settings)
}

type optionFunc func(s *settings)

func (f optionFunc) apply(s *settings) { f(s) }

// WithPageSize sets the number of persistence IDs fetched per page
// and the batch size for replaying events per entity.
// Values below one are treated as one. Default is DefaultPageSize.
func WithPageSize(size uint64) Option {
	return optionFunc(func(s *settings) {
		s.pageSize = max(size, 1)
	})
}

// WithLogger sets the logger used during migration.
func WithLogger(logger log.Logger) Option {
	return optionFunc(func(s *settings) {
		s.logger = logger
	})
}

// WithParallelism sets how many entities a migration processes at once.
// Values below one are treated as one. Default is DefaultParallelism.
func WithParallelism(parallelism int) Option {
	return optionFunc(func(s *settings) {
		s.parallelism = max(parallelism, 1)
	})
}
