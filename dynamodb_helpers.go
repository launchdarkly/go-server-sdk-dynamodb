package lddynamodb

import (
	"context"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
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

func makeClientAndContext(builder *DataStoreBuilder) (*dynamodb.Client, context.Context, context.CancelFunc, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	client := builder.client
	if client == nil {
		var config aws.Config
		if builder.awsConfig != nil {
			config = *builder.awsConfig
		} else {
			var err error
			config, err = awsconfig.LoadDefaultConfig(context.Background())
			if err != nil {
				cancelFunc()
				return nil, nil, nil, err
			}
		}
		var optFns []func(*dynamodb.Options)
		if builder.clientOptions != nil {
			optFns = append(optFns, func(o *dynamodb.Options) {
				*o = mergeDynamoDBOptions(*o, *builder.clientOptions)
			})
		}
		optFns = append(optFns, builder.clientOptFns...)
		client = dynamodb.NewFromConfig(config, optFns...)
	}
	return client, ctx, cancelFunc, nil
}

func mergeDynamoDBOptions(target, source dynamodb.Options) dynamodb.Options {
	// This awkward logic is necessary due to a design detail of the AWS SDK:
	// - Most applications will want to use the "default configuration" behavior, where AWS gets
	//   its credentials from whatever combination of environment variables, IAM roles, etc. are
	//   in effect.
	// - However, you can only get that by using LoadDefaultConfig, which gives you an aws.Config
	//   struct. So then you have to use dynamodb.NewFromConfig rather than dynamodb.New, and you
	//   can't pass an initial dynamodb.Options struct. If the application wanted to specify the
	//   latter, then we need to copy the fields from it as part of a modifier function, which is
	//   applied after the AWS SDK has already converted the aws.Config to dynamodb.Options
	//   internally.
	ret := target.Copy()
	ret.APIOptions = append(ret.APIOptions, source.APIOptions...)
	if source.ClientLogMode != 0 {
		ret.ClientLogMode = source.ClientLogMode
	}
	if source.Credentials != nil {
		ret.Credentials = source.Credentials
	}
	if source.DefaultsMode != "" {
		ret.DefaultsMode = source.DefaultsMode
	}
	if source.DisableValidateResponseChecksum {
		ret.DisableValidateResponseChecksum = source.DisableValidateResponseChecksum
	}
	if source.EnableAcceptEncodingGzip {
		ret.EnableAcceptEncodingGzip = source.EnableAcceptEncodingGzip
	}
	if source.EndpointDiscovery != (dynamodb.EndpointDiscoveryOptions{}) {
		ret.EndpointDiscovery.EnableEndpointDiscovery = source.EndpointDiscovery.EnableEndpointDiscovery
	}
	if source.EndpointOptions != (dynamodb.EndpointResolverOptions{}) {
		ret.EndpointOptions = source.EndpointOptions
	}
	if source.EndpointResolver != nil {
		ret.EndpointResolver = source.EndpointResolver
	}
	if source.HTTPClient != nil {
		ret.HTTPClient = source.HTTPClient
	}
	if source.HTTPSignerV4 != nil {
		ret.HTTPSignerV4 = source.HTTPSignerV4
	}
	if source.IdempotencyTokenProvider != nil {
		ret.IdempotencyTokenProvider = source.IdempotencyTokenProvider
	}
	if source.Logger != nil {
		ret.Logger = source.Logger
	}
	if source.Region != "" {
		ret.Region = source.Region
	}
	if source.RetryMaxAttempts != 0 {
		ret.RetryMaxAttempts = source.RetryMaxAttempts
	}
	if source.RetryMode != "" {
		ret.RetryMode = source.RetryMode
	}
	if source.Retryer != nil {
		ret.Retryer = source.Retryer
	}
	if source.RuntimeEnvironment != (aws.RuntimeEnvironment{}) {
		ret.RuntimeEnvironment = source.RuntimeEnvironment
	}
	return ret
}
