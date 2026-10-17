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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
)

const (
	// forFlag names the flag holding how long the writer runs.
	forFlag = "for"
	// defaultWriteFor is how long the writer runs by default.
	defaultWriteFor = 30 * time.Second
)

// newWriteCommand returns the command that keeps writing to the v4 journal.
func newWriteCommand() *cobra.Command {
	var (
		legacy   string
		entities int
		duration time.Duration
	)

	cmd := &cobra.Command{
		Use:   "write",
		Short: "Appends events to the v4 journal for a while, as a live v4 deployment does",
		Long: "Credits the seeded accounts one after the other for --for, each credit stamped with the current time on " +
			"the account's v4 partition, as a v4 deployment that still serves traffic during the first copy does.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pool, err := openPool(cmd.Context(), legacy)
			if err != nil {
				return err
			}
			defer pool.Close()

			return write(cmd, pool, entities, duration)
		},
	}

	cmd.Flags().StringVar(&legacy, legacyFlag, "", "connection string of the v4 database")
	cmd.Flags().IntVar(&entities, entitiesFlag, defaultEntities, "number of seeded entities")
	cmd.Flags().DurationVar(&duration, forFlag, defaultWriteFor, "how long to keep writing")
	_ = cmd.MarkFlagRequired(legacyFlag)
	return cmd
}

// write credits the seeded accounts in turn until the duration is over.
func write(cmd *cobra.Command, pool *pgxpool.Pool, entities int, duration time.Duration) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), duration)
	defer cancel()

	written := 0
	for index := 1; ctx.Err() == nil; index = index%entities + 1 {
		if isRetentionEntity(index) {
			continue
		}

		if err := appendCredit(ctx, pool, entityID(index)); err != nil {
			if ctx.Err() != nil {
				break
			}
			return err
		}
		written++
	}

	fmt.Fprintf(cmd.OutOrStdout(), "wrote %d events to the v4 journal in %s\n", written, duration)
	return nil
}

// appendCredit appends a credit to the account's v4 journal, after its latest event.
func appendCredit(ctx context.Context, pool *pgxpool.Pool, persistenceID string) error {
	_, credited, err := entityPayloads(persistenceID)
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, `INSERT INTO events_store (`+eventColumns+`)
		SELECT $1::VARCHAR, COALESCE(MAX(sequence_number), 0) + 1, FALSE, $2, $3, $4, $5, '', FALSE
		FROM events_store WHERE persistence_id = $1::VARCHAR`,
		persistenceID, credited.bytes, credited.manifest, time.Now().UnixNano(), int64(legacyShard(persistenceID)))
	return err
}
