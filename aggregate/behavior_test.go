package aggregate

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/internal/postgres"
	"github.com/tochemey/ego/storage/memory"
	pgstore "github.com/tochemey/ego/storage/postgres"
	testpb "github.com/tochemey/ego/test/data/pb/v1"
	"github.com/tochemey/goakt/actors"
	"github.com/tochemey/goakt/log"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"
)

func TestAccountAggregate(t *testing.T) {
	t.Run("with state reply", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		// create the event store
		eventStore := memory.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEntityBehavior(persistenceID)

		// create the persistence actor using the behavior previously created
		actor := New[*testpb.Account](behavior, eventStore)
		// spawn the actor
		pid := actorSystem.StartActor(ctx, behavior.ID(), actor)
		require.NotNil(t, pid)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}
		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, command, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 500.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// send another command to credit the balance
		command = &testpb.CreditAccount{
			AccountId: persistenceID,
			Balance:   250,
		}
		reply, err = actors.Ask(ctx, pid, command, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state = commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState = new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected = &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 750.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with error reply", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()

		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		// create the event store
		eventStore := memory.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEntityBehavior(persistenceID)

		// create the persistence actor using the behavior previously created
		persistentActor := New[*testpb.Account](behavior, eventStore)
		// spawn the actor
		pid := actorSystem.StartActor(ctx, behavior.ID(), persistentActor)
		require.NotNil(t, pid)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}
		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 500.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// send another command to credit the balance
		command = &testpb.CreditAccount{
			AccountId: "different-id",
			Balance:   250,
		}
		reply, err = actors.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		errorReply := commandReply.GetReply().(*egopb.CommandReply_ErrorReply)
		assert.Equal(t, "command sent to the wrong entity", errorReply.ErrorReply.GetMessage())

		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with unhandled command", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()

		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		// create the event store
		eventStore := memory.NewEventsStore()
		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEntityBehavior(persistenceID)

		// create the persistence actor using the behavior previously created
		persistentActor := New[*testpb.Account](behavior, eventStore)
		// spawn the actor
		pid := actorSystem.StartActor(ctx, behavior.ID(), persistentActor)
		require.NotNil(t, pid)

		command := &testpb.TestSend{}
		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_ErrorReply), commandReply.GetReply())

		errorReply := commandReply.GetReply().(*egopb.CommandReply_ErrorReply)
		assert.Equal(t, "unhandled command", errorReply.ErrorReply.GetMessage())
		// stop the actor system
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
	t.Run("with state recovery from event store", func(t *testing.T) {
		ctx := context.TODO()

		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		// create the event store
		var (
			testDatabase         = "testdb"
			testUser             = "testUser"
			testDatabasePassword = "testPass"
		)

		testContainer := postgres.NewTestContainer(testDatabase, testUser, testDatabasePassword)
		db := testContainer.GetTestDB()
		// create the event store table
		require.NoError(t, db.Connect(ctx))
		schemaUtils := pgstore.NewSchemaUtils(db)
		require.NoError(t, schemaUtils.CreateTable(ctx))

		config := &postgres.Config{
			DBHost:     testContainer.Host(),
			DBPort:     testContainer.Port(),
			DBName:     testDatabase,
			DBUser:     testUser,
			DBPassword: testDatabasePassword,
			DBSchema:   testContainer.Schema(),
		}
		eventStore := pgstore.NewEventsStore(config)
		require.NoError(t, eventStore.Connect(ctx))

		// create a persistence id
		persistenceID := uuid.NewString()
		// create the persistence behavior
		behavior := NewAccountEntityBehavior(persistenceID)

		// create the persistence actor using the behavior previously created
		persistentActor := New[*testpb.Account](behavior, eventStore)
		// spawn the actor
		pid := actorSystem.StartActor(ctx, behavior.ID(), persistentActor)
		require.NotNil(t, pid)

		var command proto.Message

		command = &testpb.CreateAccount{AccountBalance: 500.00}
		// send the command to the actor
		reply, err := actors.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply := reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 1, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState := new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected := &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 500.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// send another command to credit the balance
		command = &testpb.CreditAccount{
			AccountId: persistenceID,
			Balance:   250,
		}
		reply, err = actors.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		state = commandReply.GetReply().(*egopb.CommandReply_StateReply)
		assert.EqualValues(t, 2, state.StateReply.GetSequenceNumber())

		// marshal the resulting state
		resultingState = new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)

		expected = &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 750.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// shutdown the persistent actor
		assert.NoError(t, actorSystem.StopActor(ctx, behavior.ID()))
		// wait a while
		time.Sleep(time.Second)

		// restart the actor
		pid, err = actorSystem.RestartActor(ctx, behavior.ID())
		assert.NoError(t, err)

		// fetch the current state
		command = &egopb.GetStateCommand{}
		reply, err = actors.Ask(ctx, pid, command, time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)
		require.IsType(t, new(egopb.CommandReply), reply)

		commandReply = reply.(*egopb.CommandReply)
		require.IsType(t, new(egopb.CommandReply_StateReply), commandReply.GetReply())

		resultingState = new(testpb.Account)
		err = state.StateReply.GetState().UnmarshalTo(resultingState)
		require.NoError(t, err)
		expected = &testpb.Account{
			AccountId:      persistenceID,
			AccountBalance: 750.00,
		}
		assert.True(t, proto.Equal(expected, resultingState))

		// free resources
		assert.NoError(t, schemaUtils.DropTable(ctx))
		testContainer.Cleanup()
		err = actorSystem.Stop(ctx)
		assert.NoError(t, err)
	})
}
