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
	nethttp "net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	goset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/kapetan-io/tackle/autotls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	gerrors "github.com/tochemey/goakt/v4/errors"
	"github.com/tochemey/goakt/v4/supervisor"
	"github.com/travisjeffery/go-dynaport"
	"go.opentelemetry.io/otel"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventstream"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/internal/pause"
	egomock "github.com/tochemey/ego/v4/mocks/ego"
	mockoffsetstore "github.com/tochemey/ego/v4/mocks/offsetstore"
	mockpersistence "github.com/tochemey/ego/v4/mocks/persistence"
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

		provider := &mockClusterProvider{id: "id", peers: addrs}

		// create a projection message handler
		handler := projection.NewDiscardHandler()
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

		provider := &mockClusterProvider{id: "id", peers: addrs}

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

		provider := &mockClusterProvider{id: "id", peers: addrs}

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

		provider := &mockClusterProvider{id: "id", peers: addrs}

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
				EventsRetentionCount:   10,
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
	t.Run("With AddProjection as singleton in cluster mode", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		// grab dynamic ports
		nodePorts := dynaport.Get(3)
		remotingPort := nodePorts[0]
		gossipPort := nodePorts[1]
		clusterPort := nodePorts[2]

		host := "127.0.0.1"
		addrs := []string{net.JoinHostPort(host, strconv.Itoa(gossipPort))}

		provider := &mockClusterProvider{id: "id", peers: addrs}

		// create a projection message handler
		handler := projection.NewDiscardHandler()

		engine := NewEngine("Sample", eventStore,
			WithLogger(DiscardLogger),
			WithOffsetStore(offsetStore),
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
		require.NoError(t, err)

		// wait for the cluster to fully start
		pause.For(time.Second)

		// add projection — in cluster mode this should spawn a singleton
		projectionName := "singleton-projection"
		err = engine.AddProjection(ctx, projectionName)
		require.NoError(t, err)

		pause.For(time.Second)

		running, err := engine.IsProjectionRunning(ctx, projectionName)
		require.NoError(t, err)
		require.True(t, running)

		// free resources
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("With AddProjection as regular actor in standalone mode", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		offsetStore := testkit.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		// create a projection message handler
		handler := projection.NewDiscardHandler()

		// no cluster — standalone mode
		engine := NewEngine("Sample", eventStore,
			WithLogger(DiscardLogger),
			WithOffsetStore(offsetStore),
			WithProjection(&projection.Options{
				Handler:      handler,
				BufferSize:   500,
				StartOffset:  ZeroTime,
				ResetOffset:  ZeroTime,
				PullInterval: time.Second,
				Recovery:     projection.NewRecovery(),
			}))

		err := engine.Start(ctx)
		require.NoError(t, err)

		pause.For(time.Second)

		// add projection — in standalone mode this should spawn a regular long-lived actor
		projectionName := "standalone-projection"
		err = engine.AddProjection(ctx, projectionName)
		require.NoError(t, err)

		pause.For(time.Second)

		running, err := engine.IsProjectionRunning(ctx, projectionName)
		require.NoError(t, err)
		require.True(t, running)

		// free resources
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, engine.Stop(ctx))
	})
	t.Run("SendCommand with telemetry traces success and error", func(t *testing.T) {
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

		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		require.NoError(t, engine.Entity(ctx, behavior))

		resultingState, revision, err := engine.SendCommand(ctx, entityID, &samplepb.CreateAccount{
			AccountId: entityID, AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)
		require.EqualValues(t, 1, revision)
		require.NotNil(t, resultingState)

		_, _, err = engine.SendCommand(ctx, entityID, &testpb.TestSend{}, time.Minute)
		require.Error(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("SendCommand with telemetry and SendSync failure", func(t *testing.T) {
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

		_, _, err := engine.SendCommand(ctx, "non-existent-entity", &samplepb.CreateAccount{
			AccountId: "non-existent-entity", AccountBalance: 100,
		}, time.Millisecond)
		require.Error(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("Event publisher with publish error logs and continues", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		publisher := new(egomock.EventPublisher)
		published := make(chan struct{}, 2)
		publisher.On("ID").Return("eGo.test.FailingPublisher")
		publisher.On("Close", mock.Anything).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.Event")).
			Run(func(_ mock.Arguments) {
				select {
				case published <- struct{}{}:
				default:
				}
			}).
			Return(assert.AnError)

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		require.NoError(t, engine.AddEventPublishers(publisher))

		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		require.NoError(t, engine.Entity(ctx, behavior))

		_, _, err := engine.SendCommand(ctx, entityID, &samplepb.CreateAccount{
			AccountId: entityID, AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)

		select {
		case <-published:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for publish attempt")
		}

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
		publisher.AssertExpectations(t)
	})
	t.Run("State publisher with publish error logs and continues", func(t *testing.T) {
		ctx := context.TODO()
		stateStore := testkit.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))

		publisher := new(egomock.StatePublisher)
		published := make(chan struct{}, 2)
		publisher.On("ID").Return("eGo.test.FailingStatePublisher")
		publisher.On("Close", mock.Anything).Return(nil)
		publisher.
			On("Publish", mock.Anything, mock.AnythingOfType("*egopb.DurableState")).
			Run(func(_ mock.Arguments) {
				select {
				case published <- struct{}{}:
				default:
				}
			}).
			Return(assert.AnError)

		engine := NewEngine("Sample", nil,
			WithStateStore(stateStore),
			WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		require.NoError(t, engine.AddStatePublishers(publisher))

		entityID := uuid.NewString()
		behavior := NewAccountDurableStateBehavior(entityID)
		require.NoError(t, engine.DurableStateEntity(ctx, behavior))

		_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
			AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)

		select {
		case <-published:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for state publish attempt")
		}

		require.NoError(t, engine.Stop(ctx))
		assert.NoError(t, stateStore.Disconnect(ctx))
		publisher.AssertExpectations(t)
	})
	t.Run("Entity with batch threshold enables stashing", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		err := engine.Entity(ctx, behavior,
			WithBatchThreshold(5),
			WithBatchFlushWindow(10*time.Millisecond),
		)
		require.NoError(t, err)

		resultingState, revision, err := engine.SendCommand(ctx, entityID, &samplepb.CreateAccount{
			AccountId: entityID, AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)
		require.EqualValues(t, 1, revision)
		require.NotNil(t, resultingState)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("EraseEntity full with snapshot store and actual events", func(t *testing.T) {
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
		require.NoError(t, engine.Entity(ctx, behavior,
			WithSnapshotInterval(1),
		))

		_, _, err := engine.SendCommand(ctx, entityID, &samplepb.CreateAccount{
			AccountId: entityID, AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)

		_, _, err = engine.SendCommand(ctx, entityID, &samplepb.CreditAccount{
			AccountId: entityID, Balance: 50,
		}, time.Minute)
		require.NoError(t, err)

		err = engine.EraseEntity(ctx, entityID, true)
		require.NoError(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("RebuildProjection with RemoveProjection error", func(t *testing.T) {
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

		err := engine.RebuildProjection(ctx, "never-existed-projection", time.Now())
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to stop projection")

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("RebuildProjection with ResetOffset error", func(t *testing.T) {
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

		projectionName := "rebuild-offset-test"
		require.NoError(t, engine.AddProjection(ctx, projectionName))
		pause.For(time.Second)

		err := engine.RebuildProjection(ctx, projectionName, time.Now().Add(-time.Hour))
		require.NoError(t, err)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("EraseEntity GetLatestEvent error", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().GetLatestEvent(mock.Anything, mock.Anything).Return(nil, assert.AnError)
		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.mutex.Unlock()

		err := engine.EraseEntity(ctx, "entity-1", true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get latest event for erasure")

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("EraseEntity DeleteEvents error", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().GetLatestEvent(mock.Anything, mock.Anything).
			Return(&egopb.Event{PersistenceId: "entity-1", SequenceNumber: 1}, nil)
		mockEvents.EXPECT().DeleteEvents(mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.mutex.Unlock()

		err := engine.EraseEntity(ctx, "entity-1", true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to delete events for erasure")

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("EraseEntity DeleteSnapshots error", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().GetLatestEvent(mock.Anything, mock.Anything).
			Return(&egopb.Event{PersistenceId: "entity-1", SequenceNumber: 1}, nil)
		mockEvents.EXPECT().DeleteEvents(mock.Anything, mock.Anything, mock.Anything).Return(nil)

		mockSnapshots := new(mockpersistence.SnapshotStore)
		mockSnapshots.EXPECT().DeleteSnapshots(mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.snapshotStore = mockSnapshots
		engine.mutex.Unlock()

		err := engine.EraseEntity(ctx, "entity-1", true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to delete snapshots for erasure")

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.snapshotStore = nil
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag ShardNumbers error", func(t *testing.T) {
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

		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().ShardNumbers(mock.Anything).Return(nil, assert.AnError)
		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.mutex.Unlock()

		_, err := engine.ProjectionLag(ctx, "test-projection")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to fetch shard numbers")

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag GetCurrentOffset error", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{1}, nil)

		mockOffset := new(mockoffsetstore.OffsetStore)
		mockOffset.EXPECT().GetCurrentOffset(mock.Anything, mock.Anything).Return(nil, assert.AnError)

		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.offsetStore = mockOffset
		engine.mutex.Unlock()

		_, err := engine.ProjectionLag(ctx, "test-projection")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get offset for shard")

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.offsetStore = nil
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag GetShardEvents error", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{1}, nil)
		mockEvents.EXPECT().GetShardEvents(mock.Anything, uint64(1), int64(0), uint64(1)).
			Return(nil, int64(0), assert.AnError)

		mockOffset := new(mockoffsetstore.OffsetStore)
		mockOffset.EXPECT().GetCurrentOffset(mock.Anything, mock.Anything).
			Return(&egopb.Offset{Value: 0}, nil)

		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.offsetStore = mockOffset
		engine.mutex.Unlock()

		_, err := engine.ProjectionLag(ctx, "test-projection")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get latest event for shard")

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.offsetStore = nil
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag empty shard returns zero lag", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{1}, nil)
		mockEvents.EXPECT().GetShardEvents(mock.Anything, uint64(1), int64(0), uint64(1)).
			Return([]*egopb.Event{}, int64(0), nil)

		mockOffset := new(mockoffsetstore.OffsetStore)
		mockOffset.EXPECT().GetCurrentOffset(mock.Anything, mock.Anything).
			Return(&egopb.Offset{Value: 0}, nil)

		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.offsetStore = mockOffset
		engine.mutex.Unlock()

		lags, err := engine.ProjectionLag(ctx, "test-projection")
		require.NoError(t, err)
		require.Len(t, lags, 1)
		assert.EqualValues(t, 0, lags[1])

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.offsetStore = nil
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag second GetShardEvents error", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{1}, nil)
		mockEvents.EXPECT().GetShardEvents(mock.Anything, uint64(1), int64(0), uint64(1)).
			Return([]*egopb.Event{{PersistenceId: "e1", SequenceNumber: 1, Timestamp: 100}}, int64(0), nil)
		mockEvents.EXPECT().GetShardEvents(mock.Anything, uint64(1), int64(0), uint64(10000)).
			Return(nil, int64(0), assert.AnError)

		mockOffset := new(mockoffsetstore.OffsetStore)
		mockOffset.EXPECT().GetCurrentOffset(mock.Anything, mock.Anything).
			Return(&egopb.Offset{Value: 0}, nil)

		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.offsetStore = mockOffset
		engine.mutex.Unlock()

		_, err := engine.ProjectionLag(ctx, "test-projection")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get events for shard")

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.offsetStore = nil
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag offset ahead of events yields zero lag", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		now := time.Now().UnixMilli()
		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{1}, nil)
		mockEvents.EXPECT().GetShardEvents(mock.Anything, uint64(1), int64(0), uint64(1)).
			Return([]*egopb.Event{{PersistenceId: "e1", SequenceNumber: 1, Timestamp: now}}, int64(0), nil)
		mockEvents.EXPECT().GetShardEvents(mock.Anything, uint64(1), int64(0), uint64(10000)).
			Return([]*egopb.Event{{PersistenceId: "e1", SequenceNumber: 1, Timestamp: now}}, int64(0), nil)

		mockOffset := new(mockoffsetstore.OffsetStore)
		mockOffset.EXPECT().GetCurrentOffset(mock.Anything, mock.Anything).
			Return(&egopb.Offset{Value: now + 1000}, nil)

		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.offsetStore = mockOffset
		engine.mutex.Unlock()

		lags, err := engine.ProjectionLag(ctx, "test-projection")
		require.NoError(t, err)
		require.Len(t, lags, 1)
		assert.EqualValues(t, 0, lags[1])

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.offsetStore = nil
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("ProjectionLag second GetShardEvents returns empty", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		mockEvents := new(mockpersistence.EventsStore)
		mockEvents.EXPECT().ShardNumbers(mock.Anything).Return([]uint64{1}, nil)
		mockEvents.EXPECT().GetShardEvents(mock.Anything, uint64(1), int64(0), uint64(1)).
			Return([]*egopb.Event{{PersistenceId: "e1", SequenceNumber: 1, Timestamp: 100}}, int64(0), nil)
		mockEvents.EXPECT().GetShardEvents(mock.Anything, uint64(1), int64(0), uint64(10000)).
			Return([]*egopb.Event{}, int64(0), nil)

		mockOffset := new(mockoffsetstore.OffsetStore)
		mockOffset.EXPECT().GetCurrentOffset(mock.Anything, mock.Anything).
			Return(&egopb.Offset{Value: 0}, nil)

		engine.mutex.Lock()
		engine.eventsStore = mockEvents
		engine.offsetStore = mockOffset
		engine.mutex.Unlock()

		lags, err := engine.ProjectionLag(ctx, "test-projection")
		require.NoError(t, err)
		require.Len(t, lags, 1)
		assert.EqualValues(t, 0, lags[1])

		engine.mutex.Lock()
		engine.eventsStore = eventStore
		engine.offsetStore = nil
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("RebuildProjection with mock offset store ResetOffset error", func(t *testing.T) {
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

		projectionName := "rebuild-reset-err"
		require.NoError(t, engine.AddProjection(ctx, projectionName))
		pause.For(time.Second)

		mockOffset := new(mockoffsetstore.OffsetStore)
		mockOffset.EXPECT().ResetOffset(mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
		engine.mutex.Lock()
		engine.offsetStore = mockOffset
		engine.mutex.Unlock()

		err := engine.RebuildProjection(ctx, projectionName, time.Now())
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to reset offset for projection")

		engine.mutex.Lock()
		engine.offsetStore = offsetStore
		engine.mutex.Unlock()
		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, offsetStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
	t.Run("Saga spawn failure", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		sagaBehavior := &testSagaBehavior{
			sagaID:   strings.Repeat("s", 256),
			entityID: "dummy",
		}

		err := engine.Saga(ctx, sagaBehavior, 30*time.Second)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to start saga")

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, engine.Stop(ctx))
	})
}

func TestBuildSpawnOptionsFromConfig(t *testing.T) {
	t.Run("with batch threshold enables stashing", func(t *testing.T) {
		config := &spawnConfig{
			batchThreshold:      5,
			supervisorDirective: RestartDirective,
			entitiesPlacement:   RoundRobin,
		}
		opts := buildSpawnOptionsFromConfig(config)
		require.NotEmpty(t, opts)
	})
	t.Run("with passivation", func(t *testing.T) {
		config := &spawnConfig{
			passivateAfter:      time.Minute,
			supervisorDirective: RestartDirective,
			entitiesPlacement:   RoundRobin,
		}
		opts := buildSpawnOptionsFromConfig(config)
		require.NotEmpty(t, opts)
	})
	t.Run("with relocation enabled", func(t *testing.T) {
		config := &spawnConfig{
			toRelocate:          true,
			supervisorDirective: RestartDirective,
			entitiesPlacement:   RoundRobin,
		}
		opts := buildSpawnOptionsFromConfig(config)
		require.NotEmpty(t, opts)
	})
	t.Run("with all options combined", func(t *testing.T) {
		config := &spawnConfig{
			passivateAfter:      time.Minute,
			toRelocate:          false,
			batchThreshold:      10,
			supervisorDirective: StopDirective,
			entitiesPlacement:   LeastLoad,
		}
		opts := buildSpawnOptionsFromConfig(config)
		require.NotEmpty(t, opts)
	})
}

func TestClusterConfig(t *testing.T) {
	t.Run("with roles", func(t *testing.T) {
		e := &Engine{
			minimumPeersQuorum: 1,
			roles:              goset.NewSet[string]("role1", "role2"),
		}
		config := e.clusterConfig()
		require.NotNil(t, config)
	})
	t.Run("without roles", func(t *testing.T) {
		e := &Engine{
			minimumPeersQuorum: 1,
			roles:              goset.NewSet[string](),
		}
		config := e.clusterConfig()
		require.NotNil(t, config)
	})
}

func TestAppendTLSOption(t *testing.T) {
	t.Run("with TLS enabled", func(t *testing.T) {
		conf := autotls.Config{
			AutoTLS:            true,
			ClientAuth:         tls.NoClientCert,
			InsecureSkipVerify: true,
		}
		require.NoError(t, autotls.Setup(&conf))

		e := &Engine{
			tls: &TLS{
				ClientTLS: conf.ClientTLS,
				ServerTLS: conf.ServerTLS,
			},
		}
		opts := e.appendTLSOption(nil)
		require.Len(t, opts, 1)
	})
	t.Run("with TLS nil", func(t *testing.T) {
		e := &Engine{}
		opts := e.appendTLSOption([]goakt.Option{goakt.WithActorInitMaxRetries(1)})
		require.Len(t, opts, 1)
	})
}

func TestAppendClusterOptions(t *testing.T) {
	t.Run("cluster disabled returns unchanged opts", func(t *testing.T) {
		e := &Engine{}
		e.clusterEnabled.Store(false)
		original := []goakt.Option{goakt.WithActorInitMaxRetries(1)}
		opts := e.appendClusterOptions(original)
		require.Len(t, opts, 1)
	})
	t.Run("cluster enabled with telemetry includes context propagator", func(t *testing.T) {
		tracer := nooptrace.NewTracerProvider().Tracer("test")
		meter := noopmetric.NewMeterProvider().Meter("test")
		e := &Engine{
			telemetry:          &Telemetry{Tracer: tracer, Meter: meter},
			bindAddr:           "127.0.0.1",
			remotingPort:       9000,
			discoveryPort:      9001,
			peersPort:          9002,
			minimumPeersQuorum: 1,
			partitionsCount:    10,
			roles:              goset.NewSet[string](),
		}
		e.clusterEnabled.Store(true)
		opts := e.appendClusterOptions(nil)
		require.Len(t, opts, 2)
	})
	t.Run("cluster enabled without telemetry", func(t *testing.T) {
		e := &Engine{
			bindAddr:           "127.0.0.1",
			remotingPort:       9000,
			discoveryPort:      9001,
			peersPort:          9002,
			minimumPeersQuorum: 1,
			partitionsCount:    10,
			roles:              goset.NewSet[string](),
		}
		e.clusterEnabled.Store(true)
		opts := e.appendClusterOptions(nil)
		require.Len(t, opts, 2)
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

func TestOtelContextPropagator(t *testing.T) {
	// Install W3C TraceContext propagator for the duration of this test.
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	propagator := otelContextPropagator{}

	// Build a context carrying a valid span context (no SDK required).
	traceID := trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := trace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	})
	ctxWithSpan := trace.ContextWithRemoteSpanContext(context.Background(), sc)

	t.Run("Inject writes traceparent header", func(t *testing.T) {
		headers := make(nethttp.Header)
		err := propagator.Inject(ctxWithSpan, headers)
		require.NoError(t, err)
		assert.NotEmpty(t, headers.Get("Traceparent"),
			"Inject should write a traceparent header")
	})

	t.Run("Extract recovers span context from headers", func(t *testing.T) {
		headers := make(nethttp.Header)
		require.NoError(t, propagator.Inject(ctxWithSpan, headers))
		require.NotEmpty(t, headers.Get("Traceparent"))

		extractedCtx, err := propagator.Extract(context.Background(), headers)
		require.NoError(t, err)

		extracted := trace.SpanContextFromContext(extractedCtx)
		assert.Equal(t, traceID, extracted.TraceID(),
			"Extract should recover the same trace ID")
		assert.Equal(t, spanID, extracted.SpanID(),
			"Extract should recover the same span ID")
	})

	t.Run("Inject with no span writes no traceparent", func(t *testing.T) {
		headers := make(nethttp.Header)
		err := propagator.Inject(context.Background(), headers)
		require.NoError(t, err)
		assert.Empty(t, headers.Get("Traceparent"),
			"Inject without an active span should not write traceparent")
	})

	t.Run("Extract with empty headers returns invalid span context", func(t *testing.T) {
		headers := make(nethttp.Header)
		extractedCtx, err := propagator.Extract(context.Background(), headers)
		require.NoError(t, err)

		extracted := trace.SpanContextFromContext(extractedCtx)
		assert.False(t, extracted.IsValid(),
			"Extract from empty headers should not produce a valid span context")
	})

	t.Run("round-trip preserves trace context", func(t *testing.T) {
		headers := make(nethttp.Header)
		require.NoError(t, propagator.Inject(ctxWithSpan, headers))

		extractedCtx, err := propagator.Extract(context.Background(), headers)
		require.NoError(t, err)

		recovered := trace.SpanContextFromContext(extractedCtx)
		assert.Equal(t, traceID, recovered.TraceID())
		assert.Equal(t, spanID, recovered.SpanID())
		assert.True(t, recovered.IsSampled())
	})
}

func TestStartActorSystem(t *testing.T) {
	t.Run("NewActorSystem error with empty name", func(t *testing.T) {
		e := &Engine{name: ""}
		err := e.startActorSystem(context.TODO(), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to create the ego actor system")
	})
}

func TestStartFailure(t *testing.T) {
	t.Run("Start fails when actorSystemOptions errors", func(t *testing.T) {
		handler := projection.NewDiscardHandler()
		e := NewEngine("Sample", testkit.NewEventsStore(),
			WithProjection(&projection.Options{
				Handler:      handler,
				PullInterval: time.Second,
			}),
			WithLogger(DiscardLogger))
		err := e.Start(context.TODO())
		require.Error(t, err)
		require.Contains(t, err.Error(), "projection extension requires an offset store")
	})
	t.Run("Start fails when startActorSystem fails", func(t *testing.T) {
		e := NewEngine("", testkit.NewEventsStore(), WithLogger(DiscardLogger))
		err := e.Start(context.TODO())
		require.Error(t, err)
	})
}

func TestSendEvent(t *testing.T) {
	t.Run("nil message is skipped", func(t *testing.T) {
		e := &Engine{logger: DiscardLogger}
		done := make(chan Done, 1)
		sub := eventstream.New().AddSubscriber()

		stream := &eventsStream{
			publisher:  nil,
			subscriber: sub,
			done:       done,
		}

		go func() {
			time.Sleep(50 * time.Millisecond)
			done <- Done{}
		}()

		e.sendEvent(stream)
	})
	t.Run("wrong payload type is skipped", func(t *testing.T) {
		e := &Engine{logger: DiscardLogger}
		es := eventstream.New()
		sub := es.AddSubscriber()
		topic := "test-wrong-payload"
		es.Subscribe(sub, topic)

		done := make(chan Done, 1)
		stream := &eventsStream{
			publisher:  nil,
			subscriber: sub,
			done:       done,
		}

		go func() {
			es.Publish(topic, &egopb.DurableState{PersistenceId: "x"})
			time.Sleep(100 * time.Millisecond)
			done <- Done{}
		}()

		e.sendEvent(stream)
	})
}

func TestSendState(t *testing.T) {
	t.Run("nil message is skipped", func(t *testing.T) {
		e := &Engine{logger: DiscardLogger}
		done := make(chan Done, 1)
		sub := eventstream.New().AddSubscriber()

		stream := &statesStream{
			publisher:  nil,
			subscriber: sub,
			done:       done,
		}

		go func() {
			time.Sleep(50 * time.Millisecond)
			done <- Done{}
		}()

		e.sendState(stream)
	})
	t.Run("wrong payload type is skipped", func(t *testing.T) {
		e := &Engine{logger: DiscardLogger}
		es := eventstream.New()
		sub := es.AddSubscriber()
		topic := "test-wrong-state-payload"
		es.Subscribe(sub, topic)

		done := make(chan Done, 1)
		stream := &statesStream{
			publisher:  nil,
			subscriber: sub,
			done:       done,
		}

		go func() {
			es.Publish(topic, &egopb.Event{PersistenceId: "x"})
			time.Sleep(100 * time.Millisecond)
			done <- Done{}
		}()

		e.sendState(stream)
	})
}

func TestEntityExists(t *testing.T) {
	t.Run("returns ErrEngineNotStarted when engine not started", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))

		exists, err := engine.EntityExists(ctx, uuid.NewString())
		require.Error(t, err)
		require.EqualError(t, err, ErrEngineNotStarted.Error())
		require.False(t, exists)

		require.NoError(t, eventStore.Disconnect(ctx))
	})

	t.Run("returns false when entity is not found", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		exists, err := engine.EntityExists(ctx, uuid.NewString())
		require.NoError(t, err)
		require.False(t, exists)

		require.NoError(t, engine.Stop(ctx))
		require.NoError(t, eventStore.Disconnect(ctx))
	})

	t.Run("returns true for a running event-sourced entity", func(t *testing.T) {
		ctx := context.TODO()
		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		engine := NewEngine("Sample", eventStore, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		pause.For(time.Second)

		entityID := uuid.NewString()
		behavior := NewEventSourcedEntity(entityID)
		require.NoError(t, engine.Entity(ctx, behavior))

		// drive a command so the entity is fully materialized
		_, _, err := engine.SendCommand(ctx, entityID, &samplepb.CreateAccount{
			AccountId:      entityID,
			AccountBalance: 100.00,
		}, time.Minute)
		require.NoError(t, err)

		exists, err := engine.EntityExists(ctx, entityID)
		require.NoError(t, err)
		require.True(t, exists)

		require.NoError(t, engine.Stop(ctx))
		require.NoError(t, eventStore.Disconnect(ctx))
	})

}
