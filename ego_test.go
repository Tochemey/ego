package ego

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/entity"
	"github.com/tochemey/ego/eventstore/memory"
	samplepb "github.com/tochemey/ego/example/pbs/sample/pb/v1"
	"github.com/tochemey/goakt/discovery"
	mockdisco "github.com/tochemey/goakt/goaktmocks/discovery"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/protobuf/proto"
)

func TestEgo(t *testing.T) {
	t.Run("With single node cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := memory.NewEventsStore()

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

		// create the ego engine
		e := New("Sample", eventStore, WithCluster(provider, config, 4))
		// start ego engine
		err := e.Start(ctx)

		// wait for the cluster to fully start
		time.Sleep(time.Second)

		require.NoError(t, err)
		// create a persistence id
		entityID := uuid.NewString()
		// create an entity behavior with a given id
		behavior := NewAccountBehavior(entityID)
		// create an entity
		NewEntity[*samplepb.Account](ctx, e, behavior)
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
		reply, err := e.SendCommand(ctx, command, entityID)
		require.NoError(t, err)
		require.IsType(t, new(egopb.CommandReply_StateReply), reply.GetReply())
		assert.EqualValues(t, 1, reply.GetStateReply().GetSequenceNumber())

		resultingState := new(samplepb.Account)
		assert.NoError(t, reply.GetStateReply().GetState().UnmarshalTo(resultingState))
		assert.EqualValues(t, 500.00, resultingState.GetAccountBalance())
		assert.Equal(t, entityID, resultingState.GetAccountId())

		// send another command to credit the balance
		command = &samplepb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}
		reply, err = e.SendCommand(ctx, command, entityID)
		require.NoError(t, err)
		require.IsType(t, new(egopb.CommandReply_StateReply), reply.GetReply())
		assert.EqualValues(t, 2, reply.GetStateReply().GetSequenceNumber())

		newState := new(samplepb.Account)
		assert.NoError(t, reply.GetStateReply().GetState().UnmarshalTo(newState))
		assert.EqualValues(t, 750.00, newState.GetAccountBalance())
		assert.Equal(t, entityID, newState.GetAccountId())

		// free resources
		assert.NoError(t, e.Stop(ctx))
	})
	t.Run("With no cluster enabled", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := memory.NewEventsStore()
		// create the ego engine
		e := New("Sample", eventStore)
		// start ego engine
		err := e.Start(ctx)
		require.NoError(t, err)
		// create a persistence id
		entityID := uuid.NewString()
		// create an entity behavior with a given id
		behavior := NewAccountBehavior(entityID)
		// create an entity
		NewEntity[*samplepb.Account](ctx, e, behavior)
		// send some commands to the pid
		var command proto.Message
		// create an account
		command = &samplepb.CreateAccount{
			AccountId:      entityID,
			AccountBalance: 500.00,
		}
		// send the command to the actor. Please don't ignore the error in production grid code
		reply, err := e.SendCommand(ctx, command, entityID)
		require.NoError(t, err)
		require.IsType(t, new(egopb.CommandReply_StateReply), reply.GetReply())
		assert.EqualValues(t, 1, reply.GetStateReply().GetSequenceNumber())

		resultingState := new(samplepb.Account)
		assert.NoError(t, reply.GetStateReply().GetState().UnmarshalTo(resultingState))
		assert.EqualValues(t, 500.00, resultingState.GetAccountBalance())
		assert.Equal(t, entityID, resultingState.GetAccountId())

		// send another command to credit the balance
		command = &samplepb.CreditAccount{
			AccountId: entityID,
			Balance:   250,
		}
		reply, err = e.SendCommand(ctx, command, entityID)
		require.NoError(t, err)
		require.IsType(t, new(egopb.CommandReply_StateReply), reply.GetReply())
		assert.EqualValues(t, 2, reply.GetStateReply().GetSequenceNumber())

		newState := new(samplepb.Account)
		assert.NoError(t, reply.GetStateReply().GetState().UnmarshalTo(newState))
		assert.EqualValues(t, 750.00, newState.GetAccountBalance())
		assert.Equal(t, entityID, newState.GetAccountId())

		// free resources
		assert.NoError(t, e.Stop(ctx))
	})
}

// AccountBehavior implements persistence.Behavior
type AccountBehavior struct {
	id string
}

// make sure that AccountBehavior is a true persistence behavior
var _ entity.Behavior[*samplepb.Account] = &AccountBehavior{}

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
func (a *AccountBehavior) HandleCommand(_ context.Context, command entity.Command, _ *samplepb.Account) (event entity.Event, err error) {
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
func (a *AccountBehavior) HandleEvent(_ context.Context, event entity.Event, priorState *samplepb.Account) (state *samplepb.Account, err error) {
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
