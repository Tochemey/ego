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

// Package dynamodb implements the eGo events, snapshot, durable state and
// offset stores on Amazon DynamoDB.
//
// Every store runs on a *dynamodb.Client the application creates and owns, on
// a table the application creates before the engine starts with the key
// schema the store's godoc and the README give. Payloads are stored as the
// type URL and the bytes of their anypb.Any, so reading one back needs no type
// registry.
package dynamodb

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"

	"github.com/tochemey/ego/v5/persistence"
)

const (
	// transactionItemLimit is the largest number of items one TransactWriteItems request holds.
	transactionItemLimit = 100
	// batchWriteLimit is the largest number of requests one BatchWriteItem request holds.
	batchWriteLimit = 25
	// shardQueryConcurrency bounds how many per-shard queries run at once.
	shardQueryConcurrency = 32
	// unprocessedRetryDelay is the wait before resending the requests a BatchWriteItem left
	// unprocessed; it doubles after every attempt up to maxUnprocessedRetryDelay.
	unprocessedRetryDelay = 50 * time.Millisecond
	// maxUnprocessedRetryDelay caps the wait between two attempts of a BatchWriteItem.
	maxUnprocessedRetryDelay = 2 * time.Second
	// fixedWidthFormat formats a number with as many digits as the largest uint64, so
	// numbers in a string sort key order as numbers do.
	fixedWidthFormat = "%020d"
)

// ping verifies the table exists and DynamoDB answers.
func ping(ctx context.Context, client *dynamodb.Client, tableName *string) error {
	if _, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: tableName}); err != nil {
		return fmt.Errorf("dynamodb: failed to reach the table: %w", err)
	}

	return nil
}

// queryItems runs the query and follows its pages until limit items are read
// or the query has no more items. A limit of zero reads nothing.
func queryItems(ctx context.Context, client *dynamodb.Client, input *dynamodb.QueryInput, limit uint64) ([]map[string]types.AttributeValue, error) {
	if limit == 0 {
		return nil, nil
	}

	var items []map[string]types.AttributeValue
	for {
		remaining := limit - uint64(len(items))
		input.Limit = aws.Int32(int32(min(remaining, math.MaxInt32)))

		output, err := client.Query(ctx, input)
		if err != nil {
			return nil, err
		}

		items = append(items, output.Items...)
		if output.LastEvaluatedKey == nil || uint64(len(items)) >= limit {
			return items, nil
		}

		input.ExclusiveStartKey = output.LastEvaluatedKey
	}
}

// deleteItems deletes the items with the given keys, batchWriteLimit keys per
// BatchWriteItem request, resending the requests DynamoDB leaves unprocessed.
func deleteItems(ctx context.Context, client *dynamodb.Client, tableName string, keys []map[string]types.AttributeValue) error {
	for start := 0; start < len(keys); start += batchWriteLimit {
		chunk := keys[start:min(start+batchWriteLimit, len(keys))]
		requests := make([]types.WriteRequest, len(chunk))
		for index, key := range chunk {
			requests[index] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: key}}
		}

		if err := batchWrite(ctx, client, map[string][]types.WriteRequest{tableName: requests}); err != nil {
			return err
		}
	}

	return nil
}

// batchWrite sends the requests and resends the ones DynamoDB leaves
// unprocessed until none is left, waiting longer after every attempt.
func batchWrite(ctx context.Context, client *dynamodb.Client, requests map[string][]types.WriteRequest) error {
	delay := unprocessedRetryDelay
	for len(requests) > 0 {
		output, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: requests})
		if err != nil {
			return err
		}

		requests = output.UnprocessedItems
		if len(requests) == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		delay = min(2*delay, maxUnprocessedRetryDelay)
	}

	return nil
}

// forEachShard calls visit for every journal shard, at most
// shardQueryConcurrency at a time, and returns the first error.
func forEachShard(ctx context.Context, visit func(ctx context.Context, shardNumber uint64) error) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(shardQueryConcurrency)

	for shardNumber := range persistence.NumberOfShards {
		group.Go(func() error {
			return visit(groupCtx, shardNumber)
		})
	}

	return group.Wait()
}

// shardTimestamps collects the newest timestamp of the shards that have one.
type shardTimestamps struct {
	// mutex guards timestamps against the concurrent per-shard queries.
	mutex sync.Mutex
	// timestamps maps a shard number to its newest timestamp.
	timestamps map[uint64]int64
}

// set records the newest timestamp of the shard.
func (x *shardTimestamps) set(shardNumber uint64, timestamp int64) {
	x.mutex.Lock()
	x.timestamps[shardNumber] = timestamp
	x.mutex.Unlock()
}

// stringValue returns the attribute value of a string.
func stringValue(value string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: value}
}

// unsignedValue returns the attribute value of an unsigned number.
func unsignedValue(value uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(value, 10)}
}

// signedValue returns the attribute value of a signed number.
func signedValue(value int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

// boolValue returns the attribute value of a boolean.
func boolValue(value bool) types.AttributeValue {
	return &types.AttributeValueMemberBOOL{Value: value}
}

// fixedWidth formats a non-negative number so that, as strings, numbers sort
// in numeric order.
func fixedWidth[T int64 | uint64](value T) string {
	return fmt.Sprintf(fixedWidthFormat, value)
}

// lengthPrefixed prefixes value with its length, so that no length-prefixed
// value is a prefix of another one and a key built from it is unambiguous.
func lengthPrefixed(value string) string {
	return strconv.Itoa(len(value)) + ":" + value
}
