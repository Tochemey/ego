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

package dynamodb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcdynamodb "github.com/testcontainers/testcontainers-go/modules/dynamodb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// dynamodbImage is the DynamoDB Local image the tests run against.
	dynamodbImage = "amazon/dynamodb-local:3.1.0"
	// testTable is the table that holds the states in the tests.
	testTable = "states_store"
	// missingTable is a table that is never created.
	missingTable = "missing_table"
	// testRegion is the region the test client signs requests for; DynamoDB Local ignores it.
	testRegion = "us-east-1"
)

// testClient is the client of the DynamoDB Local container shared by the tests.
var testClient *dynamodb.Client

// TestMain starts one DynamoDB Local container for the package, creates the
// test table, runs the tests and removes the container.
func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcdynamodb.Run(ctx, dynamodbImage, tcdynamodb.WithDisableTelemetry())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start dynamodb: %v\n", err)
		os.Exit(1)
	}

	address, err := container.ConnectionString(ctx)
	if err == nil {
		testClient = dynamodb.New(dynamodb.Options{
			Region:       testRegion,
			BaseEndpoint: aws.String("http://" + address),
			Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		})
		err = createTable(ctx)
	}

	code := 1
	if err == nil {
		code = m.Run()
	} else {
		fmt.Fprintf(os.Stderr, "failed to prepare dynamodb: %v\n", err)
	}

	_ = container.Terminate(ctx)
	os.Exit(code)
}

func TestDurableStore(t *testing.T) {
	ctx := context.Background()
	store := NewDurableStore(testTable, testClient)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Ping(ctx))

	t.Run("returns nil when no state is stored", func(t *testing.T) {
		state, err := store.GetLatestState(ctx, "missing")
		require.NoError(t, err)
		assert.Nil(t, state)
	})

	t.Run("writes and reads the latest state", func(t *testing.T) {
		persistenceID := "account-write"
		first := newDurableState(t, persistenceID, 1, "opened")
		second := newDurableState(t, persistenceID, 2, "credited")

		require.NoError(t, store.WriteState(ctx, first))
		require.NoError(t, store.WriteState(ctx, second))

		state, err := store.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		assert.True(t, proto.Equal(second, state), "got %v", state)
	})

	t.Run("keeps a tombstone when deleting with a version", func(t *testing.T) {
		persistenceID := "account-tombstone"
		require.NoError(t, store.WriteState(ctx, newDurableState(t, persistenceID, 3, "credited")))

		require.NoError(t, store.DeleteState(ctx, persistenceID, 4))

		state, err := store.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, uint64(4), state.GetVersionNumber())
		assert.Nil(t, state.GetResultingState())
		assert.Equal(t, persistence.ShardFor(persistenceID), state.GetShard())
		assert.Positive(t, state.GetTimestamp())
	})

	t.Run("removes the item when deleting with version zero", func(t *testing.T) {
		persistenceID := "account-delete"
		require.NoError(t, store.WriteState(ctx, newDurableState(t, persistenceID, 1, "opened")))

		require.NoError(t, store.DeleteState(ctx, persistenceID, 0))

		state, err := store.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		assert.Nil(t, state)
	})

	t.Run("deleting a missing state is not an error", func(t *testing.T) {
		require.NoError(t, store.DeleteState(ctx, "never-written", 0))
	})

	t.Run("fails to decode a malformed item", func(t *testing.T) {
		persistenceID := "account-malformed"
		_, err := testClient.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(testTable),
			Item: map[string]types.AttributeValue{
				partitionKey:    &types.AttributeValueMemberS{Value: persistenceID},
				"VersionNumber": &types.AttributeValueMemberS{Value: "not a number"},
			},
		})
		require.NoError(t, err)

		state, err := store.GetLatestState(ctx, persistenceID)
		assert.Error(t, err)
		assert.Nil(t, state)
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestDurableStoreErrors(t *testing.T) {
	ctx := context.Background()
	store := NewDurableStore(missingTable, testClient)

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.WriteState(ctx, newDurableState(t, "account", 1, "opened")))
	assert.Error(t, store.DeleteState(ctx, "account", 1))
	assert.Error(t, store.DeleteState(ctx, "account", 0))

	state, err := store.GetLatestState(ctx, "account")
	assert.Error(t, err)
	assert.Nil(t, state)
}

// newDurableState builds a durable state of the persistence ID whose resulting
// state is a string value.
func newDurableState(t *testing.T, persistenceID string, version uint64, value string) *egopb.DurableState {
	t.Helper()

	resultingState, err := anypb.New(wrapperspb.String(value))
	require.NoError(t, err)

	return &egopb.DurableState{
		PersistenceId:  persistenceID,
		VersionNumber:  version,
		ResultingState: resultingState,
		Timestamp:      time.Now().UnixNano(),
		Shard:          persistence.ShardFor(persistenceID),
	}
}

// createTable creates the test table with the key schema the store requires.
func createTable(ctx context.Context) error {
	_, err := testClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(testTable),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(partitionKey), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(partitionKey), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	return err
}
