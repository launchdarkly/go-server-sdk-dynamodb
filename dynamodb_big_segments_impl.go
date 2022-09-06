package lddynamodb

import (
	"context"
	"errors"

	"gopkg.in/launchdarkly/go-sdk-common.v2/ldlog"
	"gopkg.in/launchdarkly/go-sdk-common.v2/ldtime"
	"gopkg.in/launchdarkly/go-server-sdk.v5/interfaces"
	"gopkg.in/launchdarkly/go-server-sdk.v5/ldcomponents/ldstoreimpl"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	bigSegmentsMetadataKey  = "big_segments_metadata"
	bigSegmentsUserDataKey  = "big_segments_user"
	bigSegmentsSyncTimeAttr = "synchronizedOn"
	bigSegmentsIncludedAttr = "included"
	bigSegmentsExcludedAttr = "excluded"
)

// Internal implementation of the BigSegmentStore interface for DynamoDB.
type dynamoDBBigSegmentStoreImpl struct {
	client        *dynamodb.Client
	context       context.Context
	cancelContext func()
	table         string
	prefix        string
	loggers       ldlog.Loggers
}

func newDynamoDBBigSegmentStoreImpl(
	builder *DataStoreBuilder,
	loggers ldlog.Loggers,
) (*dynamoDBBigSegmentStoreImpl, error) {
	if builder.table == "" {
		return nil, errors.New("table name is required")
	}

	client, context, cancelContext, err := makeClientAndContext(builder)
	if err != nil {
		return nil, err
	}
	store := &dynamoDBBigSegmentStoreImpl{
		client:        client,
		context:       context,
		cancelContext: cancelContext,
		table:         builder.table,
		prefix:        builder.prefix,
		loggers:       loggers, // copied by value so we can modify it
	}
	store.loggers.SetPrefix("DynamoDBBigSegmentStoreStore:")
	store.loggers.Infof(`Using DynamoDB table %s`, store.table)

	return store, nil
}

func (store *dynamoDBBigSegmentStoreImpl) GetMetadata() (interfaces.BigSegmentStoreMetadata, error) {
	key := prefixedNamespace(store.prefix, bigSegmentsMetadataKey)
	result, err := store.client.GetItem(store.context, &dynamodb.GetItemInput{
		TableName:      aws.String(store.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			tablePartitionKey: attrValueOfString(key),
			tableSortKey:      attrValueOfString(key),
		},
	})
	if err != nil {
		return interfaces.BigSegmentStoreMetadata{}, err // COVERAGE: can't cause this in unit tests
	}
	if len(result.Item) == 0 {
		return interfaces.BigSegmentStoreMetadata{}, errors.New("timestamp not found")
	}

	value := attrValueToUint64(result.Item[bigSegmentsSyncTimeAttr])
	if value == 0 {
		return interfaces.BigSegmentStoreMetadata{}, nil // COVERAGE: can't cause this in unit tests
	}

	return interfaces.BigSegmentStoreMetadata{
		LastUpToDate: ldtime.UnixMillisecondTime(value),
	}, nil
}

func (store *dynamoDBBigSegmentStoreImpl) GetUserMembership(
	userHashKey string,
) (interfaces.BigSegmentMembership, error) {
	result, err := store.client.GetItem(store.context, &dynamodb.GetItemInput{
		TableName:      aws.String(store.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			tablePartitionKey: attrValueOfString(prefixedNamespace(store.prefix, bigSegmentsUserDataKey)),
			tableSortKey:      attrValueOfString(userHashKey),
		},
	})
	if err != nil {
		return nil, err // COVERAGE: can't cause this in unit tests
	}
	if len(result.Item) == 0 {
		return ldstoreimpl.NewBigSegmentMembershipFromSegmentRefs(nil, nil), nil
	}
	includedRefs := getStringListFromSet(result.Item[bigSegmentsIncludedAttr])
	excludedRefs := getStringListFromSet(result.Item[bigSegmentsExcludedAttr])
	return ldstoreimpl.NewBigSegmentMembershipFromSegmentRefs(includedRefs, excludedRefs), nil
}

func getStringListFromSet(value types.AttributeValue) []string {
	if ss, ok := value.(*types.AttributeValueMemberSS); ok {
		return ss.Value
	}
	return nil
}

func (store *dynamoDBBigSegmentStoreImpl) Close() error {
	store.cancelContext() // stops any pending operations
	return nil
}

func prefixedNamespace(prefix, baseNamespace string) string {
	if prefix == "" {
		return baseNamespace
	}
	return prefix + ":" + baseNamespace // COVERAGE: currently the standard test suite doesn't specify a prefix
}
