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

package ego

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/tochemey/goakt/log"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/eventstore/memory"
	samplepb "github.com/tochemey/ego/example/pbs/sample/pb/v1"
	offsetstore "github.com/tochemey/ego/offsetstore/memory"
	"github.com/tochemey/ego/projection"
	"github.com/tochemey/goakt/discovery"
	mockdisco "github.com/tochemey/goakt/testkit/discovery"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/protobuf/proto"
)

func TestEgo(t *testing.T) {
	t.Run("With single node cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := memory.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))
		offsetStore := offsetstore.NewOffsetStore()
		require.NoError(t, offsetStore.Connect(ctx))

		nodePorts := dynaport.Get(3)
		gossipPort := nodePorts[0]
		clusterPort := nodePorts[1]
		remotingPort := nodePorts[2]

		podName := "pod"
		host := "127.0.0.1"

		// set the environments
		t.Setenv("GOSSIP_PORT", strconv.Itoa(gossipPort))
		t.Setenv("CLUSTER_PORT", strconv.Itoa(clusterPort))
		t.Setenv("REMOTING_PORT", strconv.Itoa(remotingPort))
		t.Setenv("NODE_NAME", podName)
		t.Setenv("NODE_IP", host)

		// define discovered addresses
		addrs := []string{
			net.JoinHostPort(host, strconv.Itoa(gossipPort)),
		}

		// mock the discovery provider
		provider := new(mockdisco.Provider)
		config := discovery.NewConfig()

		provider.EXPECT().ID().Return("testDisco")
		provider.EXPECT().Initialize().Return(nil)
		provider.EXPECT().Register().Return(nil)
		provider.EXPECT().Deregister().Return(nil)
		provider.EXPECT().SetConfig(config).Return(nil)
		provider.EXPECT().DiscoverPeers().Return(addrs, nil)
		provider.EXPECT().Close().Return(nil)

		// create a projection message handler
		handler := projection.NewDiscardHandler(log.DefaultLogger)
		// create the ego engine
		e := NewEngine("Sample", eventStore,
			WithCluster(provider, config, 4))
		// start ego engine
		err := e.Start(ctx)

		// wait for the cluster to fully start
		time.Sleep(time.Second)

		// add projection
		err = e.AddProjection(ctx, "discard", handler, offsetStore)
		require.NoError(t, err)

		// subscribe to events
		subscriber, err := e.Subscribe(ctx)
		require.NoError(t, err)
		require.NotNil(t, subscriber)

		require.NoError(t, err)
		// create a persistence id
		entityID := uuid.NewString()
		// create an entity behavior with a given id
		behavior := NewAccountBehavior(entityID)
		// create an entity
		entity, err := NewEntity[*samplepb.Account](ctx, behavior, e)
		require.NoError(t, err)
		// send some commands to the pid
		var command proto.Message
		// create an account
		command = &samplepb.CreateAccount{
			AccountId:      entityID,
			AccountBalance: 500.00,
		}

		// wait for the cluster to fully start
		time.Sleep(time.Second)

		// send the command to the actor. Please don't ignore the error in production grid code
		resultingState, revision, err := entity.SendCommand(ctx, command)
		require.NoError(t, err)

		assert.EqualValues(t, 500.00, resultingState.GetAccountBalance())
		assert.Equal(t, entityID, resultingState.GetAccountId())
		assert.EqualValues(t, 1, revision)

		// send another command to credit the balance
		command = &samplepb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}
		newState, revision, err := entity.SendCommand(ctx, command)
		require.NoError(t, err)

		assert.EqualValues(t, 750.00, newState.GetAccountBalance())
		assert.Equal(t, entityID, newState.GetAccountId())
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
		assert.NoError(t, e.Stop(ctx))
	})
	t.Run("With no cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := memory.NewEventsStore()
		// connect to the event store
		require.NoError(t, eventStore.Connect(ctx))
		// create the ego engine
		e := NewEngine("Sample", eventStore)
		// start ego engine
		err := e.Start(ctx)
		require.NoError(t, err)
		// create a persistence id
		entityID := uuid.NewString()
		// create an entity behavior with a given id
		behavior := NewAccountBehavior(entityID)
		// create an entity
		entity, err := NewEntity[*samplepb.Account](ctx, behavior, e)
		require.NoError(t, err)
		// send some commands to the pid
		var command proto.Message
		// create an account
		command = &samplepb.CreateAccount{
			AccountId:      entityID,
			AccountBalance: 500.00,
		}
		// send the command to the actor. Please don't ignore the error in production grid code
		resultingState, revision, err := entity.SendCommand(ctx, command)
		require.NoError(t, err)

		assert.EqualValues(t, 500.00, resultingState.GetAccountBalance())
		assert.Equal(t, entityID, resultingState.GetAccountId())
		assert.EqualValues(t, 1, revision)

		// send another command to credit the balance
		command = &samplepb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}
		newState, revision, err := entity.SendCommand(ctx, command)
		require.NoError(t, err)

		assert.EqualValues(t, 750.00, newState.GetAccountBalance())
		assert.Equal(t, entityID, newState.GetAccountId())
		assert.EqualValues(t, 2, revision)

		// free resources
		assert.NoError(t, eventStore.Disconnect(ctx))
		assert.NoError(t, e.Stop(ctx))
	})
}

// AccountBehavior implements persistence.Behavior
type AccountBehavior struct {
	id string
}

// make sure that AccountBehavior is a true persistence behavior
var _ EntityBehavior[*samplepb.Account] = &AccountBehavior{}

// NewAccountBehavior creates an instance of AccountBehavior
func NewAccountBehavior(id string) *AccountBehavior {
	return &AccountBehavior{id: id}
}

// ID returns the id
func (a *AccountBehavior) ID() string {
	return a.id
}

// InitialState returns the initial state
func (a *AccountBehavior) InitialState() *samplepb.Account {
	return new(samplepb.Account)
}

// HandleCommand handles every command that is sent to the persistent behavior
func (a *AccountBehavior) HandleCommand(_ context.Context, command Command, _ *samplepb.Account) (event Event, err error) {
	switch cmd := command.(type) {
	case *samplepb.CreateAccount:
		// TODO in production grid app validate the command using the prior state
		return &samplepb.AccountCreated{
			AccountId:      cmd.GetAccountId(),
			AccountBalance: cmd.GetAccountBalance(),
		}, nil

	case *samplepb.CreditAccount:
		// TODO in production grid app validate the command using the prior state
		return &samplepb.AccountCredited{
			AccountId:      cmd.GetAccountId(),
			AccountBalance: cmd.GetBalance(),
		}, nil

	default:
		return nil, errors.New("unhandled command")
	}
}

// HandleEvent handles every event emitted
func (a *AccountBehavior) HandleEvent(_ context.Context, event Event, priorState *samplepb.Account) (state *samplepb.Account, err error) {
	switch evt := event.(type) {
	case *samplepb.AccountCreated:
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: evt.GetAccountBalance(),
		}, nil

	case *samplepb.AccountCredited:
		bal := priorState.GetAccountBalance() + evt.GetAccountBalance()
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: bal,
		}, nil

	default:
		return nil, errors.New("unhandled event")
	}
}
