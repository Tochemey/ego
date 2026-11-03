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
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// createdTables numbers the tables the tests create, so every table name is unique.
var createdTables atomic.Int64

// createEventsTable creates an events table with the key schema and the indexes
// EventsStore requires, deleted when the test ends, and returns its name.
func createEventsTable(t *testing.T) string {
	t.Helper()

	return createTestTable(t, "events", &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			attribute(partitionKey, types.ScalarAttributeTypeS),
			attribute(sequenceNumberAttribute, types.ScalarAttributeTypeN),
			attribute(shardNumberAttribute, types.ScalarAttributeTypeN),
			attribute(shardKeyAttribute, types.ScalarAttributeTypeS),
			attribute(markerShardAttribute, types.ScalarAttributeTypeN),
		},
		KeySchema: keySchema(partitionKey, sequenceNumberAttribute),
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName:  aws.String(shardIndex),
				KeySchema:  keySchema(shardNumberAttribute, shardKeyAttribute),
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName:  aws.String(persistenceIDsIndex),
				KeySchema:  keySchema(markerShardAttribute, partitionKey),
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
			},
		},
	})
}

// createSnapshotsTable creates a snapshots table with the key schema
// SnapshotStore requires, deleted when the test ends, and returns its name.
func createSnapshotsTable(t *testing.T) string {
	t.Helper()

	return createTestTable(t, "snapshots", &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			attribute(partitionKey, types.ScalarAttributeTypeS),
			attribute(sequenceNumberAttribute, types.ScalarAttributeTypeN),
		},
		KeySchema: keySchema(partitionKey, sequenceNumberAttribute),
	})
}

// createStatesTable creates a durable states table with the key schema
// DurableStore requires, deleted when the test ends, and returns its name.
func createStatesTable(t *testing.T) string {
	t.Helper()

	return createTestTable(t, "states", &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{attribute(partitionKey, types.ScalarAttributeTypeS)},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String(partitionKey), KeyType: types.KeyTypeHash}},
	})
}

// createOffsetsTable creates an offsets table with the key schema and the index
// OffsetStore requires, deleted when the test ends, and returns its name.
func createOffsetsTable(t *testing.T) string {
	t.Helper()

	return createTestTable(t, "offsets", &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			attribute(offsetPartitionAttribute, types.ScalarAttributeTypeS),
			attribute(offsetKeyAttribute, types.ScalarAttributeTypeS),
			attribute(entityKeyAttribute, types.ScalarAttributeTypeS),
		},
		KeySchema: keySchema(offsetPartitionAttribute, offsetKeyAttribute),
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName:  aws.String(entityIndex),
				KeySchema:  keySchema(offsetPartitionAttribute, entityKeyAttribute),
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	})
}

// createTestTable creates a table with a unique name from the prefix, deleted
// when the test ends, and returns its name.
func createTestTable(t *testing.T, prefix string, input *dynamodb.CreateTableInput) string {
	t.Helper()

	tableName := fmt.Sprintf("%s_%d", prefix, createdTables.Add(1))
	input.TableName = aws.String(tableName)
	input.BillingMode = types.BillingModePayPerRequest

	_, err := testClient.CreateTable(context.Background(), input)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = testClient.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	})

	return tableName
}

// attribute defines a key attribute of a table or an index.
func attribute(name string, attributeType types.ScalarAttributeType) types.AttributeDefinition {
	return types.AttributeDefinition{AttributeName: aws.String(name), AttributeType: attributeType}
}

// keySchema returns the key schema of a partition key and a sort key.
func keySchema(partitionKeyName, sortKeyName string) []types.KeySchemaElement {
	return []types.KeySchemaElement{
		{AttributeName: aws.String(partitionKeyName), KeyType: types.KeyTypeHash},
		{AttributeName: aws.String(sortKeyName), KeyType: types.KeyTypeRange},
	}
}
