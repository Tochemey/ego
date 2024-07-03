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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/gopack/postgres"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstore"
	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
)

func TestPostgresEventsStore(t *testing.T) {
	t.Run("testNewEventsStore", func(t *testing.T) {
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}

		estore := NewEventsStore(config)
		assert.NotNil(t, estore)
		var p interface{} = estore
		_, ok := p.(eventstore.EventsStore)
		assert.True(t, ok)
	})
	t.Run("testConnect:happy path", func(t *testing.T) {
		ctx := context.TODO()
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}

		store := NewEventsStore(config)
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		assert.NoError(t, err)
		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testConnect:database does not exist", func(t *testing.T) {
		ctx := context.TODO()
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     "testDatabase",
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}

		store := NewEventsStore(config)
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to ping database connection: pq: database \"testDatabase\" does not exist")
	})
	t.Run("testWriteAndReplayEvents", func(t *testing.T) {
		ctx := context.TODO()
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}

		store := NewEventsStore(config)
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		require.NoError(t, err)

		db, err := dbHandle(ctx)
		require.NoError(t, err)

		schemaUtil := NewSchemaUtils(db)

		err = schemaUtil.CreateTable(ctx)
		require.NoError(t, err)

		state, err := anypb.New(&testpb.Account{})
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCreated{})
		assert.NoError(t, err)

		ts1 := timestamppb.Now()
		ts2 := timestamppb.Now()
		shard1 := uint64(5)
		shard2 := uint64(4)

		e1 := &egopb.Event{
			PersistenceId:  "persistence-1",
			SequenceNumber: 1,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      ts1.AsTime().Unix(),
			Shard:          shard1,
		}

		event, err = anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		e2 := &egopb.Event{
			PersistenceId:  "persistence-1",
			SequenceNumber: 2,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      ts2.AsTime().Unix(),
			Shard:          shard2,
		}

		events := []*egopb.Event{e1, e2}
		err = store.WriteEvents(ctx, events)
		assert.NoError(t, err)

		persistenceID := "persistence-1"
		max := uint64(4)
		from := uint64(1)
		to := uint64(2)
		replayed, err := store.ReplayEvents(ctx, persistenceID, from, to, max)
		assert.NoError(t, err)
		assert.NotEmpty(t, replayed)
		assert.Len(t, replayed, 2)
		assert.Equal(t, prototext.Format(events[0]), prototext.Format(replayed[0]))
		assert.Equal(t, prototext.Format(events[1]), prototext.Format(replayed[1]))

		shardNumbers, err := store.ShardNumbers(ctx)
		require.NoError(t, err)
		require.Len(t, shardNumbers, 2)

		offset := int64(0)
		events, nextOffset, err := store.GetShardEvents(ctx, shard1, offset, max)
		assert.NoError(t, err)
		assert.EqualValues(t, e1.GetTimestamp(), nextOffset)
		assert.Len(t, events, 1)

		err = schemaUtil.DropTable(ctx)
		assert.NoError(t, err)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testGetLatestEvent", func(t *testing.T) {
		ctx := context.TODO()
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}

		store := NewEventsStore(config)
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		require.NoError(t, err)

		db, err := dbHandle(ctx)
		require.NoError(t, err)

		schemaUtil := NewSchemaUtils(db)

		err = schemaUtil.CreateTable(ctx)
		require.NoError(t, err)

		state, err := anypb.New(&testpb.Account{})
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCreated{})
		assert.NoError(t, err)

		ts1 := timestamppb.New(time.Now().UTC())
		ts2 := timestamppb.New(time.Now().UTC())
		shard1 := uint64(7)
		shard2 := uint64(4)

		e1 := &egopb.Event{
			PersistenceId:  "persistence-1",
			SequenceNumber: 1,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      ts1.AsTime().Unix(),
			Shard:          shard1,
		}

		event, err = anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		e2 := &egopb.Event{
			PersistenceId:  "persistence-1",
			SequenceNumber: 2,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      ts2.AsTime().Unix(),
			Shard:          shard2,
		}

		events := []*egopb.Event{e1, e2}
		err = store.WriteEvents(ctx, events)
		assert.NoError(t, err)

		persistenceID := "persistence-1"

		actual, err := store.GetLatestEvent(ctx, persistenceID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, prototext.Format(e2), prototext.Format(actual))

		err = schemaUtil.DropTable(ctx)
		assert.NoError(t, err)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testDeleteEvents", func(t *testing.T) {
		ctx := context.TODO()
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}

		store := NewEventsStore(config)
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		require.NoError(t, err)

		db, err := dbHandle(ctx)
		require.NoError(t, err)

		schemaUtil := NewSchemaUtils(db)

		err = schemaUtil.CreateTable(ctx)
		require.NoError(t, err)

		state, err := anypb.New(&testpb.Account{})
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCreated{})
		assert.NoError(t, err)

		ts1 := timestamppb.New(time.Now().UTC())
		ts2 := timestamppb.New(time.Now().UTC())
		shard1 := uint64(9)
		shard2 := uint64(8)

		e1 := &egopb.Event{
			PersistenceId:  "persistence-1",
			SequenceNumber: 1,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      ts1.AsTime().Unix(),
			Shard:          shard1,
		}

		event, err = anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		e2 := &egopb.Event{
			PersistenceId:  "persistence-1",
			SequenceNumber: 2,
			IsDeleted:      false,
			Event:          event,
			ResultingState: state,
			Timestamp:      ts2.AsTime().Unix(),
			Shard:          shard2,
		}

		events := []*egopb.Event{e1, e2}
		err = store.WriteEvents(ctx, events)
		assert.NoError(t, err)

		persistenceID := "persistence-1"

		actual, err := store.GetLatestEvent(ctx, persistenceID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, prototext.Format(e2), prototext.Format(actual))

		// let us delete the events
		err = store.DeleteEvents(ctx, persistenceID, uint64(3))
		assert.NoError(t, err)
		actual, err = store.GetLatestEvent(ctx, persistenceID)
		assert.NoError(t, err)
		assert.Nil(t, actual)

		err = schemaUtil.DropTable(ctx)
		assert.NoError(t, err)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
	t.Run("testShardNumbers", func(t *testing.T) {
		ctx := context.TODO()
		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}

		store := NewEventsStore(config)
		assert.NotNil(t, store)
		err := store.Connect(ctx)
		require.NoError(t, err)

		db, err := dbHandle(ctx)
		require.NoError(t, err)

		schemaUtil := NewSchemaUtils(db)

		err = schemaUtil.CreateTable(ctx)
		require.NoError(t, err)

		shardNumbers, err := store.ShardNumbers(ctx)
		require.NoError(t, err)
		require.Empty(t, shardNumbers)

		err = schemaUtil.DropTable(ctx)
		assert.NoError(t, err)

		err = store.Disconnect(ctx)
		assert.NoError(t, err)
	})
}
