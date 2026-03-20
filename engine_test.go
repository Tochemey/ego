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
	goakt "github.com/tochemey/goakt/v4/actor"
	gerrors "github.com/tochemey/goakt/v4/errors"
	mockdisco "github.com/tochemey/goakt/v4/mocks/discovery"
	"github.com/tochemey/goakt/v4/supervisor"
	"github.com/travisjeffery/go-dynaport"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/encryption"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/internal/pause"
	egomock "github.com/tochemey/ego/v4/mocks/ego"
	"github.com/tochemey/ego/v4/projection"
	testpb "github.com/tochemey/ego/v4/test/data/testpb"
	testkit "github.com/tochemey/ego/v4/testkit"
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
			net.JoinHostPort(host, strconv.Itoa(clusterPort)),
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
			InsecureSkipVerify: true,
		}
		require.NoError(t, autotls.Setup(&conf))

		engine := NewEngine("Sample", eventStore,
			WithLogger(DiscardLogger),
			WithTLS(&TLS{
				ClientTLS: conf.ClientTLS,
				ServerTLS: conf.ServerTLS,
			}),
			WithOffsetStore(offsetStore),
			WithRoles("role1", "role2"),
			WithProjection(&projection.Options{
				Handler:      handler,
				BufferSize:   500,
				StartOffset:  ZeroTime,
				ResetOffset:  ZeroTime,
				PullInterval: time.Second,
				Recovery:     projection.NewRecovery(),
			}),
			WithCluster(provider, 4, 1, host, remotingPort, gossipPort, clusterPort))
		err := engine.Start(ctx)

		// wait for the cluster to fully start
		pause.For(time.Second)

		// add projection
		err = engine.AddProjection(ctx, "discard")
		require.NoError(t, err)

		pause.For(time.Second)

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
		pause.For(time.Second)

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
		published := make(chan struct{}, 2)
		publisher.On("ID").Return("eGo.test.EventsPublisher")
		publisher.On("Close", ctx).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.Event")).
			Run(func(args mock.Arguments) {
				select {
				case published <- struct{}{}:
				default:
				}
			}).
			Return(nil)

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		// start ego engine
		err := engine.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

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

		// wait for both events to be published before stopping
		timeout := time.After(5 * time.Second)
		for i := 0; i < 2; i++ {
			select {
			case <-published:
			case <-timeout:
				t.Fatal("timed out waiting for events to be published")
			}
		}

		// free resources
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
		publisher.AssertExpectations(t)
	})
	t.Run("EventSourced entity With SendCommand when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
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
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
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
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		err := engine.Start(ctx)
		require.NoError(t, err)

		// create a persistence id
		entityID := uuid.NewString()

		_, _, err = engine.SendCommand(ctx, entityID, new(samplepb.CreateAccount), time.Minute)
		require.Error(t, err)
		assert.ErrorIs(t, err, gerrors.ErrActorNotFound)

		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("EventSourced entity With IsProjectionRunning when not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))

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
			WithProjection(&projection.Options{
				Handler:      handler,
				BufferSize:   500,
				StartOffset:  ZeroTime,
				ResetOffset:  ZeroTime,
				PullInterval: time.Second,
				Recovery:     projection.NewRecovery(),
			}),
			WithLogger(DiscardLogger))
		// start ego engine
		err := engine.Start(ctx)
		require.NoError(t, err)

		// add projection
		projectionName := "projection"
		err = engine.AddProjection(ctx, projectionName)
		require.NoError(t, err)

		pause.For(time.Second)

		running, err := engine.IsProjectionRunning(ctx, projectionName)
		require.NoError(t, err)
		require.True(t, running)

		err = engine.RemoveProjection(ctx, projectionName)
		require.NoError(t, err)

		pause.For(time.Second)

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
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))

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
			WithLogger(DiscardLogger),
			WithStateStore(stateStore),
			WithCluster(provider, 4, 1, host, remotingPort, gossipPort, clusterPort))

		err := engine.Start(ctx)

		// wait for the cluster to fully start
		pause.For(time.Second)

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
		pause.For(time.Second)

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
		pause.For(time.Second)
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
			WithLogger(DiscardLogger))

		err := engine.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

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
		pause.For(time.Second)
		assert.NoError(t, stateStore.Disconnect(ctx))
	})
	t.Run("DurableStore entity With SendCommand when not started", func(t *testing.T) {
		ctx := context.TODO()

		stateStore := testkit.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(DiscardLogger))

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
			WithLogger(DiscardLogger))
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
			WithLogger(DiscardLogger))
		err := engine.Start(ctx)
		require.NoError(t, err)

		// create a persistence id
		entityID := uuid.NewString()

		_, _, err = engine.SendCommand(ctx, entityID, new(testpb.CreateAccount), time.Minute)
		require.Error(t, err)
		assert.ErrorIs(t, err, gerrors.ErrActorNotFound)
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
		published := make(chan struct{}, 2)
		publisher.On("ID").Return("eGo.test.EventsPublisher")
		publisher.On("Close", ctx).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.Event")).
			Run(func(args mock.Arguments) {
				select {
				case published <- struct{}{}:
				default:
				}
			}).
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
			InsecureSkipVerify: true,
		}
		require.NoError(t, autotls.Setup(&conf))

		engine := NewEngine("Sample", eventStore,
			WithLogger(DiscardLogger),
			WithTLS(&TLS{
				ClientTLS: conf.ClientTLS,
				ServerTLS: conf.ServerTLS,
			}),
			WithOffsetStore(offsetStore),
			WithProjection(&projection.Options{
				Handler:      handler,
				BufferSize:   500,
				StartOffset:  ZeroTime,
				ResetOffset:  ZeroTime,
				PullInterval: time.Second,
				Recovery:     projection.NewRecovery(),
			}),
			WithCluster(provider, 4, 1, host, remotingPort, discoveryPort, clusterPort))

		// start ego engine
		err := engine.Start(ctx)

		// wait for the cluster to fully start
		pause.For(time.Second)

		// add the events publisher before start processing events
		err = engine.AddEventPublishers(publisher)
		require.NoError(t, err)

		// add projection
		err = engine.AddProjection(ctx, "discard")
		require.NoError(t, err)

		pause.For(time.Second)

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
		pause.For(time.Second)

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

		for message := range subscriber.Iterator() {
			payload := message.Payload()
			envelope, ok := payload.(*egopb.Event)
			event := envelope.GetEvent()
			require.True(t, ok)
			switch envelope.GetSequenceNumber() {
			case 1:
				require.True(t, event.MessageIs(new(samplepb.AccountCreated)))
			case 2:
				require.True(t, event.MessageIs(new(samplepb.AccountCredited)))
			}
		}

		// wait for both events to be published before stopping
		timeout := time.After(5 * time.Second)
		for i := 0; i < 2; i++ {
			select {
			case <-published:
			case <-timeout:
				t.Fatal("timed out waiting for events to be published")
			}
		}

		// free resources
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))

		publisher.AssertExpectations(t)
		provider.AssertExpectations(t)
	})
	t.Run("With DurableState Publisher with no cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		// mock the state publisher
		publisher := new(egomock.StatePublisher)
		published := make(chan struct{}, 2)
		publisher.On("ID").Return("eGo.test.StatePublisher")
		publisher.On("Close", ctx).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.DurableState")).
			Run(func(args mock.Arguments) {
				select {
				case published <- struct{}{}:
				default:
				}
			}).
			Return(func(ctx context.Context, state *egopb.DurableState) error {
				return nil
			})

		// create the ego engine
		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(DiscardLogger))

		err := engine.Start(ctx)
		require.NoError(t, err)

		// wait for complete start
		pause.For(time.Second)

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

		// wait for both state changes to be published before stopping
		timeout := time.After(5 * time.Second)
		for i := 0; i < 2; i++ {
			select {
			case <-published:
			case <-timeout:
				t.Fatal("timed out waiting for state changes to be published")
			}
		}

		require.NoError(t, engine.Stop(ctx))
		assert.NoError(t, stateStore.Disconnect(ctx))
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
		published := make(chan struct{}, 2)
		publisher.On("ID").Return("eGo.test.StatePublisher")
		publisher.On("Close", ctx).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.DurableState")).
			Run(func(args mock.Arguments) {
				select {
				case published <- struct{}{}:
				default:
				}
			}).
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
			WithLogger(DiscardLogger),
			WithStateStore(stateStore),
			WithCluster(provider, 4, 1, host, remotingPort, gossipPort, clusterPort))

		err := engine.Start(ctx)

		// wait for the cluster to fully start
		pause.For(time.Second)

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
		pause.For(time.Second)

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

		// wait for both state changes to be published before stopping
		timeout := time.After(5 * time.Second)
		for i := 0; i < 2; i++ {
			select {
			case <-published:
			case <-timeout:
				t.Fatal("timed out waiting for state changes to be published")
			}
		}

		// free resources
		require.NoError(t, engine.Stop(ctx))
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
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
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
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
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
			WithLogger(DiscardLogger))

		err := engine.Start(ctx)
		require.NoError(t, err)

		// wait for complete start
		pause.For(time.Second)

		// add the state publisher before start processing durable state
		err = engine.AddEventPublishers(publisher)
		require.NoError(t, err)
		require.ErrorIs(t, engine.Stop(ctx), closeErr)
		assert.NoError(t, stateStore.Disconnect(ctx))
		pause.For(time.Second)
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
			WithLogger(DiscardLogger))

		err := engine.Start(ctx)
		require.NoError(t, err)

		// wait for complete start
		pause.For(time.Second)

		// add the state publisher before start processing durable state
		err = engine.AddStatePublishers(publisher)
		require.NoError(t, err)
		require.ErrorIs(t, engine.Stop(ctx), closeErr)
		assert.NoError(t, stateStore.Disconnect(ctx))
		pause.For(time.Second)
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
			WithProjection(&projection.Options{
				Handler:      handler,
				BufferSize:   500,
				StartOffset:  ZeroTime,
				ResetOffset:  ZeroTime,
				PullInterval: time.Second,
				Recovery:     projection.NewRecovery(),
			}),
			WithLogger(DiscardLogger))

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
			WithProjection(&projection.Options{
				Handler:      handler,
				BufferSize:   500,
				StartOffset:  ZeroTime,
				ResetOffset:  ZeroTime,
				PullInterval: time.Second,
				Recovery:     projection.NewRecovery(),
			}),
			WithLogger(DiscardLogger))

		// start ego engine
		err := engine.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

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
			WithLogger(DiscardLogger))

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
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
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
			WithLogger(DiscardLogger))

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
			WithLogger(DiscardLogger))

		require.NoError(t, engine.Start(ctx))

		entityID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(entityID)

		err := engine.DurableStateEntity(ctx, behavior)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDurableStateStoreRequired)

		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("EraseEntity when not started", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		err := engine.EraseEntity(ctx, "entity-1", false)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("EraseEntity with full erasure", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore,
			WithSnapshotStore(snapshotStore),
			WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		// create an entity and send a command
		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		require.NoError(t, engine.Entity(ctx, behavior))

		_, _, err := engine.SendCommand(ctx, entityID, &samplepb.CreateAccount{
			AccountId:      entityID,
			AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)

		// erase the entity
		err = engine.EraseEntity(ctx, entityID, true)
		require.NoError(t, err)

		// verify events are deleted
		latest, err := eventStore.GetLatestEvent(ctx, entityID)
		require.NoError(t, err)
		assert.Nil(t, latest)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("EraseEntity without full erasure", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		err := engine.EraseEntity(ctx, "entity-1", false)
		require.NoError(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag when not started", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		_, err := engine.ProjectionLag(ctx, "projection")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("ProjectionLag without offset store", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		_, err := engine.ProjectionLag(ctx, "projection")
		require.Error(t, err)
		require.Contains(t, err.Error(), "offset store is required")

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag with events and offsets", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore,
			WithOffsetStore(offsetStore),
			WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		// create an entity and send commands to produce events
		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		require.NoError(t, engine.Entity(ctx, behavior))

		_, _, err := engine.SendCommand(ctx, entityID, &samplepb.CreateAccount{
			AccountId:      entityID,
			AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)

		lags, err := engine.ProjectionLag(ctx, "test-projection")
		require.NoError(t, err)
		require.NotNil(t, lags)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("Saga with integration test", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		// create an event-sourced entity that the saga can command
		entityID := uuid.NewString()
		entityBehavior := NewEventSourcedEntity(entityID)
		require.NoError(t, engine.Entity(ctx, entityBehavior))

		// create a saga behavior
		sagaBehavior := &testSagaBehavior{
			sagaID:   "test-saga-" + uuid.NewString(),
			entityID: entityID,
		}

		// start the saga
		err := engine.Saga(ctx, sagaBehavior, 30*time.Second)
		require.NoError(t, err)

		pause.For(2 * time.Second)

		// query saga status
		info, err := engine.SagaStatus(ctx, sagaBehavior.sagaID, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.Equal(t, sagaBehavior.sagaID, info.ID)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("Saga when not started", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		err := engine.Saga(ctx, nil, time.Minute)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("SagaStatus when not started", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		_, err := engine.SagaStatus(ctx, "saga-1", time.Minute)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("SagaStatus with empty sagaID", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		_, err := engine.SagaStatus(ctx, "", time.Minute)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrUndefinedEntityID)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("IsProjectionRunning when projection does not exist", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		_, err := engine.IsProjectionRunning(ctx, "non-existent")
		require.Error(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("SagaStatus with non-existent saga", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		_, err := engine.SagaStatus(ctx, "non-existent-saga", time.Second)
		require.Error(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("SendCommand with error from entity", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		require.NoError(t, engine.Entity(ctx, behavior))

		// send an unhandled command type to trigger error reply
		_, _, err := engine.SendCommand(ctx, entityID, &testpb.TestSend{}, time.Minute)
		require.Error(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag with complete data", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore,
			WithOffsetStore(offsetStore),
			WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		// create entity and send commands to produce events with known timestamps
		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		require.NoError(t, engine.Entity(ctx, behavior))

		_, _, err := engine.SendCommand(ctx, entityID, &samplepb.CreateAccount{
			AccountId: entityID, AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)

		_, _, err = engine.SendCommand(ctx, entityID, &samplepb.CreditAccount{
			AccountId: entityID, Balance: 50,
		}, time.Minute)
		require.NoError(t, err)

		// write an offset so there's a lag
		shards, err := eventStore.ShardNumbers(ctx)
		require.NoError(t, err)
		for _, shard := range shards {
			require.NoError(t, offsetStore.WriteOffset(ctx, &egopb.Offset{
				ProjectionName: "lag-test",
				ShardNumber:    shard,
				Value:          1,
			}))
		}

		lags, err := engine.ProjectionLag(ctx, "lag-test")
		require.NoError(t, err)
		require.NotNil(t, lags)
		// at least one shard should have a lag
		hasLag := false
		for _, lag := range lags {
			if lag > 0 {
				hasLag = true
				break
			}
		}
		assert.True(t, hasLag)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("EraseEntity full with snapshot store and no events", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore,
			WithSnapshotStore(snapshotStore),
			WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		// erase a non-existent entity (no events, no snapshots)
		err := engine.EraseEntity(ctx, "non-existent-entity", true)
		require.NoError(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("RebuildProjection when not started", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		err := engine.RebuildProjection(ctx, "projection", time.Now())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("RebuildProjection successful", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		handler := projection.NewDiscardHandler()
		engine := NewEngine("Sample", eventStore,
			WithOffsetStore(offsetStore),
			WithProjection(&projection.Options{
				Handler:      handler,
				BufferSize:   500,
				StartOffset:  ZeroTime,
				ResetOffset:  ZeroTime,
				PullInterval: time.Second,
				Recovery:     projection.NewRecovery(),
			}),
			WithLogger(DiscardLogger))

		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		projectionName := "test-projection"
		require.NoError(t, engine.AddProjection(ctx, projectionName))
		pause.For(time.Second)

		// verify projection is running
		running, err := engine.IsProjectionRunning(ctx, projectionName)
		require.NoError(t, err)
		require.True(t, running)

		// rebuild the projection
		err = engine.RebuildProjection(ctx, projectionName, time.Now().Add(-time.Hour))
		require.NoError(t, err)

		pause.For(time.Second)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("RebuildProjection without offset store", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		err := engine.RebuildProjection(ctx, "projection", time.Now())
		require.Error(t, err)
		require.Contains(t, err.Error(), "offset store is required")

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("Start with telemetry setup", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		tracer := nooptrace.NewTracerProvider().Tracer("test")
		meter := noopmetric.NewMeterProvider().Meter("test")
		tel := &Telemetry{Tracer: tracer, Meter: meter}

		engine := NewEngine("Sample", eventStore,
			WithTelemetry(tel),
			WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		require.NotNil(t, engine.metrics)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("Entity with spawn config options", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore,
			WithSnapshotStore(snapshotStore),
			WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		err := engine.Entity(ctx, behavior,
			WithSnapshotInterval(5),
			WithRetentionPolicy(RetentionPolicy{
				DeleteEventsOnSnapshot: true,
				EventsRetentionCount:  10,
			}),
			WithPassivateAfter(time.Minute),
		)
		require.NoError(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("EraseEntity without snapshot store full erasure", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		require.NoError(t, engine.Entity(ctx, behavior))

		_, _, err := engine.SendCommand(ctx, entityID, &samplepb.CreateAccount{
			AccountId: entityID, AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)

		// Full erase without snapshot store - should still succeed
		err = engine.EraseEntity(ctx, entityID, true)
		require.NoError(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("appendOptionalExtensions with projection but no offset store", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()

		handler := projection.NewDiscardHandler()
		engine := NewEngine("Sample", eventStore,
			WithProjection(&projection.Options{
				Handler:      handler,
				PullInterval: time.Second,
			}),
			WithLogger(DiscardLogger))

		err := engine.Start(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "projection extension requires an offset store")
	})
	t.Run("Start with all optional extensions", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))
		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		keyStore := testkit.NewKeyStore()
		encryptor := encryption.NewAESEncryptor(keyStore)

		tracer := nooptrace.NewTracerProvider().Tracer("test")
		meter := noopmetric.NewMeterProvider().Meter("test")

		handler := projection.NewDiscardHandler()

		engine := NewEngine("Sample", eventStore,
			WithStateStore(testkit.NewDurableStore()),
			WithOffsetStore(offsetStore),
			WithSnapshotStore(snapshotStore),
			WithEncryptor(encryptor),
			WithTelemetry(&Telemetry{Tracer: tracer, Meter: meter}),
			WithEventAdapters(&testEventAdapter{}),
			WithProjection(&projection.Options{
				Handler:      handler,
				PullInterval: time.Second,
			}),
			WithLogger(DiscardLogger))

		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
}

func TestToSpawnPlacement(t *testing.T) {
	testcases := []struct {
		name      string
		input     EntitiesPlacement
		placement goakt.SpawnPlacement
	}{
		{name: "least load", input: LeastLoad, placement: goakt.LeastLoad},
		{name: "round robin", input: RoundRobin, placement: goakt.RoundRobin},
		{name: "random", input: Random, placement: goakt.Random},
		{name: "local", input: Local, placement: goakt.Local},
		{name: "default", input: EntitiesPlacement(42), placement: goakt.RoundRobin},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.placement, toSpawnPlacement(tc.input))
		})
	}
}

func TestToSupervisorDirective(t *testing.T) {
	testcases := []struct {
		name      string
		input     SupervisorDirective
		directive supervisor.Directive
	}{
		{name: "stop", input: StopDirective, directive: supervisor.StopDirective},
		{name: "restart", input: RestartDirective, directive: supervisor.RestartDirective},
		{name: "default", input: SupervisorDirective(42), directive: supervisor.RestartDirective},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.directive, toSupervisorDirective(tc.input))
		})
	}
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

func TestParseCommandReply(t *testing.T) {
	t.Run("with error reply", func(t *testing.T) {
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_ErrorReply{
				ErrorReply: &egopb.ErrorReply{Message: "something failed"},
			},
		}
		_, _, err := parseCommandReply(reply)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "something failed")
	})

	t.Run("with no reply set", func(t *testing.T) {
		reply := &egopb.CommandReply{}
		_, _, err := parseCommandReply(reply)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no state received")
	})

	t.Run("with state reply", func(t *testing.T) {
		state, _ := anypb.New(&samplepb.Account{AccountId: "acc-1", AccountBalance: 100})
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  "entity-1",
					State:          state,
					SequenceNumber: 5,
				},
			},
		}
		result, seq, err := parseCommandReply(reply)
		require.NoError(t, err)
		assert.EqualValues(t, 5, seq)
		assert.NotNil(t, result)
	})
	t.Run("with unmarshal failure", func(t *testing.T) {
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  "entity-1",
					State:          &anypb.Any{TypeUrl: "type.googleapis.com/invalid.Type", Value: []byte("garbage")},
					SequenceNumber: 1,
				},
			},
		}
		_, _, err := parseCommandReply(reply)
		require.Error(t, err)
	})
}

func TestClusterReplicaCount(t *testing.T) {
	t.Run("quorum of 1", func(t *testing.T) {
		e := &Engine{minimumPeersQuorum: 1}
		assert.EqualValues(t, 1, e.clusterReplicaCount())
	})
	t.Run("quorum greater than 1", func(t *testing.T) {
		e := &Engine{minimumPeersQuorum: 3}
		assert.EqualValues(t, 2, e.clusterReplicaCount())
	})
}

func TestClusterIntervals(t *testing.T) {
	t.Run("quorum 1", func(t *testing.T) {
		e := &Engine{minimumPeersQuorum: 1}
		sync, balance := e.clusterIntervals()
		assert.Equal(t, 10*time.Second, sync)
		assert.Equal(t, 500*time.Millisecond, balance)
	})
	t.Run("quorum 2", func(t *testing.T) {
		e := &Engine{minimumPeersQuorum: 2}
		sync, balance := e.clusterIntervals()
		assert.Equal(t, 20*time.Second, sync)
		assert.Equal(t, time.Second, balance)
	})
	t.Run("quorum 4", func(t *testing.T) {
		e := &Engine{minimumPeersQuorum: 4}
		sync, balance := e.clusterIntervals()
		assert.Equal(t, 30*time.Second, sync)
		assert.Equal(t, 2*time.Second, balance)
	})
}

func TestEnsureBindAddr(t *testing.T) {
	t.Run("empty sets default", func(t *testing.T) {
		e := &Engine{}
		e.ensureBindAddr()
		assert.Equal(t, "0.0.0.0", e.bindAddr)
	})
	t.Run("non-empty preserved", func(t *testing.T) {
		e := &Engine{bindAddr: "192.168.1.1"}
		e.ensureBindAddr()
		assert.Equal(t, "192.168.1.1", e.bindAddr)
	})
}
