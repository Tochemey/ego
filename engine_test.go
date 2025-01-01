/*
 * MIT License
 *
 * Copyright (c) 2022-2025 Arsene Tochemey Gandote
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

package ego

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v2/actors"
	"github.com/tochemey/goakt/v2/log"
	mockdisco "github.com/tochemey/goakt/v2/mocks/discovery"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3/egopb"
	samplepb "github.com/tochemey/ego/v3/example/pbs/sample/pb/v1"
	"github.com/tochemey/ego/v3/internal/lib"
	"github.com/tochemey/ego/v3/projection"
	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
	testkit2 "github.com/tochemey/ego/v3/testkit"
)

func TestEgo(t *testing.T) {
	t.Run("EventSourced entity With single node cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit2.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		offsetStore := testkit2.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		nodePorts := dynaport.Get(3)
		gossipPort := nodePorts[0]
		clusterPort := nodePorts[1]
		remotingPort := nodePorts[2]

		host := "127.0.0.1"

		// define discovered addresses
		addrs := []string{
			net.JoinHostPort(host, strconv.Itoa(gossipPort)),
		}

		// mock the discovery provider
		provider := new(mockdisco.Provider)

		provider.EXPECT().ID().Return("testDisco")
		provider.EXPECT().Initialize().Return(nil)
		provider.EXPECT().Register().Return(nil)
		provider.EXPECT().Deregister().Return(nil)
		provider.EXPECT().DiscoverPeers().Return(addrs, nil)
		provider.EXPECT().Close().Return(nil)

		// create a projection message handler
		handler := projection.NewDiscardHandler(log.DiscardLogger)
		// create the ego engine
		engine := NewEngine("Sample", eventStore,
			WithLogger(log.DiscardLogger),
			WithCluster(provider, 4, 1, host, remotingPort, gossipPort, clusterPort))
		// start ego engine
		err := engine.Start(ctx)

		// wait for the cluster to fully start
		lib.Pause(time.Second)

		// add projection
		err = engine.AddProjection(ctx, "discard", handler, offsetStore)
		require.NoError(t, err)

		lib.Pause(time.Second)

		running, err := engine.IsProjectionRunning(ctx, "discard")
		require.NoError(t, err)
		require.True(t, running)

		// subscribe to events
		subscriber, err := engine.Subscribe()
		require.NoError(t, err)
		require.NotNil(t, subscriber)

		require.NoError(t, err)
		// create a persistence id
		entityID := uuid.NewString()
		// create an entity behavior with a given id
		behavior := NewEventSourcedEntity(entityID)
		// create an entity
		err = engine.Entity(ctx, behavior)
		require.NoError(t, err)
		// send some commands to the pid
		var command proto.Message
		// create an account
		command = &samplepb.CreateAccount{
			AccountId:      entityID,
			AccountBalance: 500.00,
		}

		// wait for the cluster to fully start
		lib.Pause(time.Second)

		// send the command to the actor. Please don't ignore the error in production grid code
		resultingState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		account, ok := resultingState.(*samplepb.Account)
		require.True(t, ok)

		assert.EqualValues(t, 500.00, account.GetAccountBalance())
		assert.Equal(t, entityID, account.GetAccountId())
		assert.EqualValues(t, 1, revision)

		// send another command to credit the balance
		command = &samplepb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}

		newState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		newAccount, ok := newState.(*samplepb.Account)
		require.True(t, ok)

		assert.EqualValues(t, 750.00, newAccount.GetAccountBalance())
		assert.Equal(t, entityID, newAccount.GetAccountId())
		assert.EqualValues(t, 2, revision)

		for message := range subscriber.Iterator() {
			payload := message.Payload()
			envelope, ok := payload.(*egopb.Event)
			event := envelope.GetEvent()
			require.True(t, ok)
			switch envelope.GetSequenceNumber() {
			case 1:
				assert.True(t, event.MessageIs(new(samplepb.AccountCreated)))
			case 2:
				assert.True(t, event.MessageIs(new(samplepb.AccountCredited)))
			}
		}

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("EventSourced entity With no cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit2.NewEventsStore()
		// connect to the event store
		require.NoError(t, eventStore.Connect(ctx))
		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		// start ego engine
		err := engine.Start(ctx)
		require.NoError(t, err)
		// create a persistence id
		entityID := uuid.NewString()
		// create an entity behavior with a given id
		behavior := NewEventSourcedEntity(entityID)
		// create an entity
		err = engine.Entity(ctx, behavior)
		require.NoError(t, err)
		// send some commands to the pid
		var command proto.Message
		// create an account
		command = &samplepb.CreateAccount{
			AccountId:      entityID,
			AccountBalance: 500.00,
		}
		// send the command to the actor. Please don't ignore the error in production grid code
		resultingState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		account, ok := resultingState.(*samplepb.Account)
		require.True(t, ok)

		assert.EqualValues(t, 500.00, account.GetAccountBalance())
		assert.Equal(t, entityID, account.GetAccountId())
		assert.EqualValues(t, 1, revision)

		// send another command to credit the balance
		command = &samplepb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}
		newState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		newAccount, ok := newState.(*samplepb.Account)
		require.True(t, ok)

		assert.EqualValues(t, 750.00, newAccount.GetAccountBalance())
		assert.Equal(t, entityID, newAccount.GetAccountId())
		assert.EqualValues(t, 2, revision)

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("EventSourced entity With SendCommand when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit2.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		// create a persistence id
		entityID := uuid.NewString()

		_, _, err := engine.SendCommand(ctx, entityID, new(samplepb.CreateAccount), time.Minute)
		require.Error(t, err)
		assert.EqualError(t, err, ErrEngineNotStarted.Error())

		assert.NoError(t, eventStore.Disconnect(ctx))
	})
	t.Run("EventSourced entity With SendCommand when entityID is not set", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit2.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		err := engine.Start(ctx)
		require.NoError(t, err)

		// create a persistence id
		entityID := ""

		_, _, err = engine.SendCommand(ctx, entityID, new(samplepb.CreateAccount), time.Minute)
		require.Error(t, err)
		assert.EqualError(t, err, ErrUndefinedEntityID.Error())

		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("EventSourced entity With SendCommand when entity is not found", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit2.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		err := engine.Start(ctx)
		require.NoError(t, err)

		// create a persistence id
		entityID := uuid.NewString()

		_, _, err = engine.SendCommand(ctx, entityID, new(samplepb.CreateAccount), time.Minute)
		require.Error(t, err)
		assert.EqualError(t, err, actors.ErrActorNotFound(entityID).Error())

		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("EventSourced entity With IsProjectionRunning when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit2.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))

		running, err := engine.IsProjectionRunning(ctx, "isProjectionRunning")
		require.Error(t, err)
		assert.EqualError(t, err, ErrEngineNotStarted.Error())
		assert.False(t, running)

		assert.NoError(t, eventStore.Disconnect(ctx))
	})
	t.Run("EventSourced entity With RemoveProjection", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit2.NewEventsStore()
		// connect to the event store
		require.NoError(t, eventStore.Connect(ctx))

		offsetStore := testkit2.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		// start ego engine
		err := engine.Start(ctx)
		require.NoError(t, err)

		// create a projection message handler
		handler := projection.NewDiscardHandler(log.DiscardLogger)
		// add projection
		projectionName := "projection"
		err = engine.AddProjection(ctx, projectionName, handler, offsetStore)
		require.NoError(t, err)

		lib.Pause(time.Second)

		running, err := engine.IsProjectionRunning(ctx, projectionName)
		require.NoError(t, err)
		require.True(t, running)

		err = engine.RemoveProjection(ctx, projectionName)
		require.NoError(t, err)

		lib.Pause(time.Second)

		running, err = engine.IsProjectionRunning(ctx, projectionName)
		require.Error(t, err)
		require.False(t, running)

		// free resources
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("EventSourced entity With RemoveProjection when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit2.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))

		err := engine.RemoveProjection(ctx, "isProjectionRunning")
		require.Error(t, err)
		assert.EqualError(t, err, ErrEngineNotStarted.Error())

		assert.NoError(t, eventStore.Disconnect(ctx))
	})

	t.Run("DurableStore entity With single node cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit2.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		nodePorts := dynaport.Get(3)
		gossipPort := nodePorts[0]
		clusterPort := nodePorts[1]
		remotingPort := nodePorts[2]

		host := "127.0.0.1"

		// define discovered addresses
		addrs := []string{
			net.JoinHostPort(host, strconv.Itoa(gossipPort)),
		}

		// mock the discovery provider
		provider := new(mockdisco.Provider)

		provider.EXPECT().ID().Return("testDisco")
		provider.EXPECT().Initialize().Return(nil)
		provider.EXPECT().Register().Return(nil)
		provider.EXPECT().Deregister().Return(nil)
		provider.EXPECT().DiscoverPeers().Return(addrs, nil)
		provider.EXPECT().Close().Return(nil)

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithLogger(log.DiscardLogger),
			WithStateStore(stateStore),
			WithCluster(provider, 4, 1, host, remotingPort, gossipPort, clusterPort))

		err := engine.Start(ctx)

		// wait for the cluster to fully start
		lib.Pause(time.Second)

		// subscribe to events
		subscriber, err := engine.Subscribe()
		require.NoError(t, err)
		require.NotNil(t, subscriber)

		require.NoError(t, err)
		// create a persistence id
		entityID := uuid.NewString()

		behavior := NewAccountDurableStateBehavior(entityID)
		// create an entity
		err = engine.DurableStateEntity(ctx, behavior)
		require.NoError(t, err)
		// send some commands to the pid
		var command proto.Message
		command = &testpb.CreateAccount{
			AccountBalance: 500.00,
		}

		// wait for the cluster to fully start
		lib.Pause(time.Second)

		// send the command to the actor. Please don't ignore the error in production grid code
		resultingState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		account, ok := resultingState.(*testpb.Account)
		require.True(t, ok)

		assert.EqualValues(t, 500.00, account.GetAccountBalance())
		assert.Equal(t, entityID, account.GetAccountId())
		assert.EqualValues(t, 1, revision)

		command = &testpb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}

		newState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		newAccount, ok := newState.(*testpb.Account)
		require.True(t, ok)

		assert.EqualValues(t, 750.00, newAccount.GetAccountBalance())
		assert.Equal(t, entityID, newAccount.GetAccountId())
		assert.EqualValues(t, 2, revision)

		for message := range subscriber.Iterator() {
			payload := message.Payload()
			envelope, ok := payload.(*egopb.DurableState)
			require.True(t, ok)
			require.NotZero(t, envelope.GetVersionNumber())
		}

		// free resources
		require.NoError(t, engine.Stop(ctx))
		lib.Pause(time.Second)
		require.NoError(t, stateStore.Disconnect(ctx))
	})
	t.Run("DurableStore entity With no cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit2.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(log.DiscardLogger))

		err := engine.Start(ctx)
		require.NoError(t, err)

		entityID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(entityID)

		err = engine.DurableStateEntity(ctx, behavior)
		require.NoError(t, err)
		var command proto.Message

		command = &testpb.CreateAccount{
			AccountBalance: 500.00,
		}

		resultingState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		account, ok := resultingState.(*testpb.Account)
		require.True(t, ok)

		assert.EqualValues(t, 500.00, account.GetAccountBalance())
		assert.Equal(t, entityID, account.GetAccountId())
		assert.EqualValues(t, 1, revision)

		// send another command to credit the balance
		command = &testpb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}
		newState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		newAccount, ok := newState.(*testpb.Account)
		require.True(t, ok)

		assert.EqualValues(t, 750.00, newAccount.GetAccountBalance())
		assert.Equal(t, entityID, newAccount.GetAccountId())
		assert.EqualValues(t, 2, revision)

		assert.NoError(t, engine.Stop(ctx))
		lib.Pause(time.Second)
		assert.NoError(t, stateStore.Disconnect(ctx))
	})
	t.Run("DurableStore entity With SendCommand when not started", func(t *testing.T) {
		ctx := context.TODO()

		stateStore := testkit2.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(log.DiscardLogger))

		entityID := uuid.NewString()

		_, _, err := engine.SendCommand(ctx, entityID, new(testpb.CreateAccount), time.Minute)
		require.Error(t, err)
		assert.EqualError(t, err, ErrEngineNotStarted.Error())

		assert.NoError(t, stateStore.Disconnect(ctx))
	})
	t.Run("DurableStore entity With SendCommand when entityID is not set", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit2.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(log.DiscardLogger))
		err := engine.Start(ctx)
		require.NoError(t, err)

		// create a persistence id
		entityID := ""

		_, _, err = engine.SendCommand(ctx, entityID, new(testpb.CreateAccount), time.Minute)
		require.Error(t, err)
		assert.EqualError(t, err, ErrUndefinedEntityID.Error())
		assert.NoError(t, engine.Stop(ctx))
		assert.NoError(t, stateStore.Disconnect(ctx))
	})
	t.Run("DurableStore entity With SendCommand when entity is not found", func(t *testing.T) {
		ctx := context.TODO()

		stateStore := testkit2.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(log.DiscardLogger))
		err := engine.Start(ctx)
		require.NoError(t, err)

		// create a persistence id
		entityID := uuid.NewString()

		_, _, err = engine.SendCommand(ctx, entityID, new(testpb.CreateAccount), time.Minute)
		require.Error(t, err)
		assert.EqualError(t, err, actors.ErrActorNotFound(entityID).Error())
		assert.NoError(t, engine.Stop(ctx))
		assert.NoError(t, stateStore.Disconnect(ctx))
	})
}

// EventSourcedEntity implements persistence.Behavior
type EventSourcedEntity struct {
	id string
}

// make sure that EventSourcedEntity is a true persistence behavior
var _ EventSourcedBehavior = &EventSourcedEntity{}

// NewEventSourcedEntity creates an instance of EventSourcedEntity
func NewEventSourcedEntity(id string) *EventSourcedEntity {
	return &EventSourcedEntity{id: id}
}

// ID returns the id
func (a *EventSourcedEntity) ID() string {
	return a.id
}

// InitialState returns the initial state
func (a *EventSourcedEntity) InitialState() State {
	return State(new(samplepb.Account))
}

// HandleCommand handles every command that is sent to the persistent behavior
func (a *EventSourcedEntity) HandleCommand(_ context.Context, command Command, _ State) (events []Event, err error) {
	switch cmd := command.(type) {
	case *samplepb.CreateAccount:
		// TODO in production grid app validate the command using the prior state
		return []Event{
			&samplepb.AccountCreated{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: cmd.GetAccountBalance(),
			},
		}, nil

	case *samplepb.CreditAccount:
		// TODO in production grid app validate the command using the prior state
		return []Event{
			&samplepb.AccountCredited{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: cmd.GetBalance(),
			},
		}, nil

	default:
		return nil, errors.New("unhandled command")
	}
}

// HandleEvent handles every event emitted
func (a *EventSourcedEntity) HandleEvent(_ context.Context, event Event, priorState State) (state State, err error) {
	switch evt := event.(type) {
	case *samplepb.AccountCreated:
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: evt.GetAccountBalance(),
		}, nil

	case *samplepb.AccountCredited:
		account := priorState.(*samplepb.Account)
		bal := account.GetAccountBalance() + evt.GetAccountBalance()
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: bal,
		}, nil

	default:
		return nil, errors.New("unhandled event")
	}
}
