package lddynamodb

import (
	"context"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func attrValueOfString(value string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: value}
}

func attrValueOfInt(value int) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.Itoa(value)}
}

func attrValueToString(value types.AttributeValue) string {
	switch v := value.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	default:
		return ""
	}
}

func attrValueToInt(value types.AttributeValue) int {
	switch v := value.(type) {
	case *types.AttributeValueMemberN:
		if n, err := strconv.Atoi(v.Value); err == nil {
			return n
		}
		return 0
	default:
		return 0
	}
}

func attrValueToUint64(value types.AttributeValue) uint64 {
	switch v := value.(type) {
	case *types.AttributeValueMemberN:
		if n, err := strconv.ParseUint(v.Value, 10, 64); err == nil {
			return n
		}
		return 0
	default:
		return 0
	}
}

// batchWriteRequests executes a list of write requests (PutItem or DeleteItem)
// in batches of 25, which is the maximum BatchWriteItem can handle.
func batchWriteRequests(
	context context.Context,
	client *dynamodb.Client,
	table string,
	requests []types.WriteRequest,
) error {
	for len(requests) > 0 {
		batchSize := int(math.Min(float64(len(requests)), 25))
		batch := requests[:batchSize]
		requests = requests[batchSize:]

		_, err := client.BatchWriteItem(context, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{table: batch},
		})
		if err != nil {
			// COVERAGE: can't simulate this condition in unit tests because we will only get this
			// far if the initial query in Init() already succeeded, and we don't have the ability
			// to make DynamoDB fail *selectively* within a single test
			return err
		}
	}
	return nil
}

func makeClientAndContext(builder *DataStoreBuilder) (*dynamodb.Client, context.Context, context.CancelFunc) {
	context, cancelFunc := context.WithCancel(context.Background())
	client := builder.client
	if client == nil {
		if builder.awsConfig != nil {
			client = dynamodb.NewFromConfig(*builder.awsConfig, builder.clientOpFns...)
		} else {
			client = dynamodb.New(builder.clientOptions, builder.clientOpFns...)
		}
	}
	return client, context, cancelFunc
}
