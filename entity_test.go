package ego

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/ego/eventstore/memory"
	samplepb "github.com/tochemey/ego/example/pbs/sample/pb/v1"
)

func TestNewEntity(t *testing.T) {
	t.Run("With engine not defined", func(t *testing.T) {
		ctx := context.TODO()
		behavior := NewAccountBehavior(uuid.NewString())
		// create an entity
		entity, err := NewEntity[*samplepb.Account](ctx, behavior, nil)
		require.Error(t, err)
		require.Nil(t, entity)
		assert.EqualError(t, err, ErrEngineRequired.Error())
	})
	t.Run("With engine not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := memory.NewEventsStore()
		// create the ego engine
		engine := NewEngine("Sample", eventStore)
		behavior := NewAccountBehavior(uuid.NewString())
		// create an entity
		entity, err := NewEntity[*samplepb.Account](ctx, behavior, engine)
		require.Error(t, err)
		require.Nil(t, entity)
		assert.EqualError(t, err, ErrEngineNotStarted.Error())
	})
}

func TestSendCommand(t *testing.T) {
	t.Run("With entity not defined", func(t *testing.T) {
		ctx := context.TODO()
		entity := &Entity[*samplepb.Account]{}
		resultingState, revision, err := entity.SendCommand(ctx, new(samplepb.CreateAccount))
		require.Nil(t, resultingState)
		require.Zero(t, revision)
		require.Error(t, err)
		assert.EqualError(t, err, ErrUndefinedEntity.Error())
	})
}
