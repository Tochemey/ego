/*
 * MIT License
 *
 * Copyright (c) 2023-2025 Tochemey
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
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kapetan-io/tackle/autotls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	actors "github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/log"
	mockdisco "github.com/tochemey/goakt/v3/mocks/discovery"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3/egopb"
	samplepb "github.com/tochemey/ego/v3/example/pbs/sample/pb/v1"
	"github.com/tochemey/ego/v3/internal/lib"
	egomock "github.com/tochemey/ego/v3/mocks/ego"
	"github.com/tochemey/ego/v3/projection"
	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
	testkit "github.com/tochemey/ego/v3/testkit"
)

// nolint
func TestEngine(t *testing.T) {
	t.Run("EventSourced entity With single node cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		offsetStore := testkit.NewOffsetStore()
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

		provider.EXPECT().ID().Return("id")
		provider.EXPECT().Initialize().Return(nil)
		provider.EXPECT().Register().Return(nil)
		provider.EXPECT().Deregister().Return(nil)
		provider.EXPECT().DiscoverPeers().Return(addrs, nil)
		provider.EXPECT().Close().Return(nil)

		// create a projection message handler
		handler := projection.NewDiscardHandler()
		// create the ego engine
		// AutoGenerate TLS certs
		conf := autotls.Config{
			AutoTLS:            true,
			ClientAuth:         tls.NoClientCert,
			InsecureSkipVerify: false,
		}
		require.NoError(t, autotls.Setup(&conf))

		engine := NewEngine("Sample", eventStore,
			WithLogger(log.DiscardLogger),
			WithTLS(&TLS{
				ClientTLS: conf.ClientTLS,
				ServerTLS: conf.ServerTLS,
			}),
			WithOffsetStore(offsetStore),
			WithProjection(handler, 500, ZeroTime, ZeroTime, time.Second, projection.NewRecovery()),
			WithCluster(provider, 4, 1, host, remotingPort, gossipPort, clusterPort))
		// start ego engine
		err := engine.Start(ctx)

		// wait for the cluster to fully start
		lib.Pause(time.Second)

		// add projection
		err = engine.AddProjection(ctx, "discard")
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
		err = engine.Entity(ctx, behavior, WithPassivateAfter(time.Hour))
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
		provider.AssertExpectations(t)
	})
	t.Run("EventSourced entity With no cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		// connect to the event store
		require.NoError(t, eventStore.Connect(ctx))

		// mock the event publisher
		publisher := new(egomock.EventPublisher)
		publisher.On("ID").Return("eGo.test.EventsPublisher")
		publisher.On("Close", ctx).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.Event")).
			Return(func(ctx context.Context, event *egopb.Event) error {
				return nil
			})

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		// start ego engine
		err := engine.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// add the events publisher before start processing events
		err = engine.AddEventPublishers(publisher)
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

		require.EqualValues(t, 500.00, account.GetAccountBalance())
		require.Equal(t, entityID, account.GetAccountId())
		require.EqualValues(t, 1, revision)

		// send another command to credit the balance
		command = &samplepb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}
		newState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		newAccount, ok := newState.(*samplepb.Account)
		require.True(t, ok)

		require.EqualValues(t, 750.00, newAccount.GetAccountBalance())
		require.Equal(t, entityID, newAccount.GetAccountId())
		require.EqualValues(t, 2, revision)

		// free resources
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
		lib.Pause(time.Second)
		publisher.AssertExpectations(t)
	})
	t.Run("EventSourced entity With SendCommand when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		// create a persistence id
		entityID := uuid.NewString()

		_, _, err := engine.SendCommand(ctx, entityID, new(samplepb.CreateAccount), time.Minute)
		require.Error(t, err)
		require.EqualError(t, err, ErrEngineNotStarted.Error())

		require.NoError(t, eventStore.Disconnect(ctx))
	})
	t.Run("EventSourced entity With SendCommand when entityID is not set", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
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
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		err := engine.Start(ctx)
		require.NoError(t, err)

		// create a persistence id
		entityID := uuid.NewString()

		_, _, err = engine.SendCommand(ctx, entityID, new(samplepb.CreateAccount), time.Minute)
		require.Error(t, err)
		assert.ErrorIs(t, err, actors.ErrActorNotFound)

		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("EventSourced entity With IsProjectionRunning when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
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
		eventStore := testkit.NewEventsStore()
		// connect to the event store
		require.NoError(t, eventStore.Connect(ctx))

		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		// create a projection message handler
		handler := projection.NewDiscardHandler()

		// create the ego engine
		engine := NewEngine("Sample", eventStore,
			WithOffsetStore(offsetStore),
			WithProjection(handler, 500, ZeroTime, ZeroTime, time.Second, projection.NewRecovery()),
			WithLogger(log.DiscardLogger))
		// start ego engine
		err := engine.Start(ctx)
		require.NoError(t, err)

		// add projection
		projectionName := "projection"
		err = engine.AddProjection(ctx, projectionName)
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
		eventStore := testkit.NewEventsStore()
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
		stateStore := testkit.NewDurableStore()
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

		provider.EXPECT().ID().Return("id")
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

		// subscribe to durable state
		subscriber, err := engine.Subscribe()
		require.NoError(t, err)
		require.NotNil(t, subscriber)

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
		provider.AssertExpectations(t)
	})
	t.Run("DurableStore entity With no cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(log.DiscardLogger))

		err := engine.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// subscribe to durable state
		subscriber, err := engine.Subscribe()
		require.NoError(t, err)
		require.NotNil(t, subscriber)

		entityID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(entityID)

		err = engine.DurableStateEntity(ctx, behavior, WithPassivateAfter(time.Hour))
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

		for message := range subscriber.Iterator() {
			payload := message.Payload()
			envelope, ok := payload.(*egopb.DurableState)
			require.True(t, ok)
			require.NotZero(t, envelope.GetVersionNumber())
		}

		assert.NoError(t, engine.Stop(ctx))
		lib.Pause(time.Second)
		assert.NoError(t, stateStore.Disconnect(ctx))
	})
	t.Run("DurableStore entity With SendCommand when not started", func(t *testing.T) {
		ctx := context.TODO()

		stateStore := testkit.NewDurableStore()
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
		stateStore := testkit.NewDurableStore()
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

		stateStore := testkit.NewDurableStore()
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
		assert.ErrorIs(t, err, actors.ErrActorNotFound)
		assert.NoError(t, engine.Stop(ctx))
		assert.NoError(t, stateStore.Disconnect(ctx))
	})
	t.Run("With Events Publisher with cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		nodePorts := dynaport.Get(3)
		discoveryPort := nodePorts[0]
		clusterPort := nodePorts[1]
		remotingPort := nodePorts[2]

		host := "127.0.0.1"

		// define discovered addresses
		addrs := []string{
			net.JoinHostPort(host, strconv.Itoa(discoveryPort)),
		}

		// mock the discovery provider
		provider := new(mockdisco.Provider)

		provider.EXPECT().ID().Return("id")
		provider.EXPECT().Initialize().Return(nil)
		provider.EXPECT().Register().Return(nil)
		provider.EXPECT().Deregister().Return(nil)
		provider.EXPECT().DiscoverPeers().Return(addrs, nil)
		provider.EXPECT().Close().Return(nil)

		// mock the event publisher
		publisher := new(egomock.EventPublisher)
		publisher.On("ID").Return("eGo.test.EventsPublisher")
		publisher.On("Close", ctx).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.Event")).
			Return(func(ctx context.Context, event *egopb.Event) error {
				return nil
			})

		// create a projection message handler
		handler := projection.NewDiscardHandler()
		// create the ego engine
		// AutoGenerate TLS certs
		conf := autotls.Config{
			AutoTLS:            true,
			ClientAuth:         tls.NoClientCert,
			InsecureSkipVerify: false,
		}
		require.NoError(t, autotls.Setup(&conf))

		engine := NewEngine("Sample", eventStore,
			WithLogger(log.DiscardLogger),
			WithTLS(&TLS{
				ClientTLS: conf.ClientTLS,
				ServerTLS: conf.ServerTLS,
			}),
			WithOffsetStore(offsetStore),
			WithProjection(handler, 500, ZeroTime, ZeroTime, time.Second, projection.NewRecovery()),
			WithCluster(provider, 4, 1, host, remotingPort, discoveryPort, clusterPort))

		// start ego engine
		err := engine.Start(ctx)

		// wait for the cluster to fully start
		lib.Pause(time.Second)

		// add the events publisher before start processing events
		err = engine.AddEventPublishers(publisher)
		require.NoError(t, err)

		// add projection
		err = engine.AddProjection(ctx, "discard")
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
		lib.Pause(time.Second)

		publisher.AssertExpectations(t)
		provider.AssertExpectations(t)
	})
	t.Run("With DurableState Publisher with no cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// mock the state publisher
		publisher := new(egomock.StatePublisher)
		publisher.On("ID").Return("eGo.test.StatePublisher")
		publisher.On("Close", ctx).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.DurableState")).
			Return(func(ctx context.Context, state *egopb.DurableState) error {
				return nil
			})

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(log.DiscardLogger))

		err := engine.Start(ctx)
		require.NoError(t, err)

		// wait for complete start
		lib.Pause(time.Second)

		// add the state publisher before start processing durable state
		err = engine.AddStatePublishers(publisher)
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

		require.EqualValues(t, 500.00, account.GetAccountBalance())
		require.Equal(t, entityID, account.GetAccountId())
		require.EqualValues(t, 1, revision)

		// send another command to credit the balance
		command = &testpb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}
		newState, revision, err := engine.SendCommand(ctx, entityID, command, time.Minute)
		require.NoError(t, err)
		newAccount, ok := newState.(*testpb.Account)
		require.True(t, ok)

		require.EqualValues(t, 750.00, newAccount.GetAccountBalance())
		require.Equal(t, entityID, newAccount.GetAccountId())
		require.EqualValues(t, 2, revision)

		require.NoError(t, engine.Stop(ctx))
		assert.NoError(t, stateStore.Disconnect(ctx))
		lib.Pause(time.Second)
		publisher.AssertExpectations(t)
	})
	t.Run("With DurableState Publisher with cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit.NewDurableStore()
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

		// mock the state publisher
		publisher := new(egomock.StatePublisher)
		publisher.On("ID").Return("eGo.test.StatePublisher")
		publisher.On("Close", ctx).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.DurableState")).
			Return(func(ctx context.Context, state *egopb.DurableState) error {
				return nil
			})

		// mock the discovery provider
		provider := new(mockdisco.Provider)

		provider.EXPECT().ID().Return("id")
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

		// add the state publisher before start processing durable state
		err = engine.AddStatePublishers(publisher)
		require.NoError(t, err)

		// subscribe to durable state
		subscriber, err := engine.Subscribe()
		require.NoError(t, err)
		require.NotNil(t, subscriber)

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
		publisher.AssertExpectations(t)
		provider.AssertExpectations(t)
	})
	t.Run("With DurableState Publisher when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		publisher := new(egomock.StatePublisher)

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		err := engine.AddStatePublishers(publisher)
		require.Error(t, err)
		assert.EqualError(t, err, ErrEngineNotStarted.Error())

		assert.NoError(t, eventStore.Disconnect(ctx))
	})
	t.Run("With EventPublisher when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		publisher := new(egomock.EventPublisher)

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		err := engine.AddEventPublishers(publisher)
		require.Error(t, err)
		assert.EqualError(t, err, ErrEngineNotStarted.Error())

		assert.NoError(t, eventStore.Disconnect(ctx))
	})
	t.Run("With Engine Stop failure when EventPublisher close fails", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// mock the state publisher
		closeErr := errors.New("close error")
		publisher := new(egomock.EventPublisher)
		publisher.On("ID").Return("eGo.test.EventPublisher")
		publisher.On("Close", ctx).Return(closeErr)

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(log.DiscardLogger))

		err := engine.Start(ctx)
		require.NoError(t, err)

		// wait for complete start
		lib.Pause(time.Second)

		// add the state publisher before start processing durable state
		err = engine.AddEventPublishers(publisher)
		require.NoError(t, err)
		require.ErrorIs(t, engine.Stop(ctx), closeErr)
		assert.NoError(t, stateStore.Disconnect(ctx))
		lib.Pause(time.Second)
		publisher.AssertExpectations(t)
	})
	t.Run("With Engine Stop failure when DurableState Publisher close fails", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// mock the state publisher
		closeErr := errors.New("close error")
		publisher := new(egomock.StatePublisher)
		publisher.On("ID").Return("eGo.test.StatePublisher")
		publisher.On("Close", ctx).Return(closeErr)

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(log.DiscardLogger))

		err := engine.Start(ctx)
		require.NoError(t, err)

		// wait for complete start
		lib.Pause(time.Second)

		// add the state publisher before start processing durable state
		err = engine.AddStatePublishers(publisher)
		require.NoError(t, err)
		require.ErrorIs(t, engine.Stop(ctx), closeErr)
		assert.NoError(t, stateStore.Disconnect(ctx))
		lib.Pause(time.Second)
		publisher.AssertExpectations(t)
	})
	t.Run("With AddProjection when engine not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		offsetStore := testkit.NewOffsetStore()

		// create a projection message handler
		handler := projection.NewDiscardHandler()

		engine := NewEngine("Sample",
			eventStore,
			WithOffsetStore(offsetStore),
			WithProjection(handler, 500, ZeroTime, ZeroTime, time.Second, projection.NewRecovery()),
			WithLogger(log.DiscardLogger))

		projectionName := "projection"
		err := engine.AddProjection(ctx, projectionName)
		require.Error(t, err)
	})
	t.Run("With AddProjection when projection name is invalid", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		// connect to the event store
		require.NoError(t, eventStore.Connect(ctx))

		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		// create a projection message handler
		handler := projection.NewDiscardHandler()
		// create the ego engine
		engine := NewEngine("Sample", eventStore,
			WithOffsetStore(offsetStore),
			WithProjection(handler, 500, ZeroTime, ZeroTime, time.Second, projection.NewRecovery()),
			WithLogger(log.DiscardLogger))

		// start ego engine
		err := engine.Start(ctx)
		require.NoError(t, err)

		lib.Pause(time.Second)

		// add projection
		projectionName := strings.Repeat("a", 256)
		err = engine.AddProjection(ctx, projectionName)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to register the projection")

		// free resources
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("With Subscribe when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore,
			WithLogger(log.DiscardLogger))

		// subscribe to events
		subscriber, err := engine.Subscribe()
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEngineNotStarted)
		require.Nil(t, subscriber)

		// free resources
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("EventSourced entity when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(log.DiscardLogger))
		// create a persistence id
		entityID := uuid.NewString()
		// create an entity behavior with a given id
		behavior := NewEventSourcedEntity(entityID)

		err := engine.Entity(ctx, behavior)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEngineNotStarted)

		require.NoError(t, eventStore.Disconnect(ctx))
	})
	t.Run("DurableStore entity when not started", func(t *testing.T) {
		ctx := context.TODO()

		stateStore := testkit.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(log.DiscardLogger))

		entityID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(entityID)

		err := engine.DurableStateEntity(ctx, behavior)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEngineNotStarted)

		require.NoError(t, stateStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("DurableStore entity when durable store not set", func(t *testing.T) {
		ctx := context.TODO()

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithLogger(log.DiscardLogger))

		require.NoError(t, engine.Start(ctx))

		entityID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(entityID)

		err := engine.DurableStateEntity(ctx, behavior)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDurableStateStoreRequired)

		require.NoError(t, engine.Stop(ctx))
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

func (a *EventSourcedEntity) MarshalBinary() (data []byte, err error) {
	serializable := struct {
		ID string `json:"id"`
	}{
		ID: a.id,
	}
	return json.Marshal(serializable)
}

func (a *EventSourcedEntity) UnmarshalBinary(data []byte) error {
	serializable := struct {
		ID string `json:"id"`
	}{}

	if err := json.Unmarshal(data, &serializable); err != nil {
		return err
	}

	a.id = serializable.ID
	return nil
}
