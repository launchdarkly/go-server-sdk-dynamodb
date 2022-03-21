package lddynamodb

import (
	"errors"
	"strconv"

	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-sdk-common/v3/ldtime"
	"github.com/launchdarkly/go-server-sdk/v6/interfaces"
	"github.com/launchdarkly/go-server-sdk/v6/ldcomponents/ldstoreimpl"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
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
	client  dynamodbiface.DynamoDBAPI
	table   string
	prefix  string
	loggers ldlog.Loggers
}

func newDynamoDBBigSegmentStoreImpl(
	builder *DataStoreBuilder,
	loggers ldlog.Loggers,
) (*dynamoDBBigSegmentStoreImpl, error) {
	if builder.table == "" {
		return nil, errors.New("table name is required")
	}

	client, err := makeClient(builder)
	if err != nil {
		return nil, err
	}

	store := &dynamoDBBigSegmentStoreImpl{
		client:  client,
		table:   builder.table,
		prefix:  builder.prefix,
		loggers: loggers, // copied by value so we can modify it
	}
	store.loggers.SetPrefix("DynamoDBBigSegmentStoreStore:")
	store.loggers.Infof(`Using DynamoDB table %s`, store.table)

	return store, nil
}

func (store *dynamoDBBigSegmentStoreImpl) GetMetadata() (interfaces.BigSegmentStoreMetadata, error) {
	key := prefixedNamespace(store.prefix, bigSegmentsMetadataKey)
	result, err := store.client.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(store.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			tablePartitionKey: {S: aws.String(key)},
			tableSortKey:      {S: aws.String(key)},
		},
	})
	if err != nil {
		return interfaces.BigSegmentStoreMetadata{}, err // COVERAGE: can't cause this in unit tests
	}
	if len(result.Item) == 0 {
		return interfaces.BigSegmentStoreMetadata{}, errors.New("timestamp not found")
	}

	itemValue := result.Item[bigSegmentsSyncTimeAttr]
	if itemValue == nil || itemValue.N == nil {
		return interfaces.BigSegmentStoreMetadata{}, nil // COVERAGE: can't cause this in unit tests
	}
	value, _ := strconv.Atoi(*itemValue.N)

	return interfaces.BigSegmentStoreMetadata{
		LastUpToDate: ldtime.UnixMillisecondTime(value),
	}, nil
}

func (store *dynamoDBBigSegmentStoreImpl) GetMembership(
	contextHashKey string,
) (interfaces.BigSegmentMembership, error) {
	result, err := store.client.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(store.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			tablePartitionKey: {S: aws.String(prefixedNamespace(store.prefix, bigSegmentsUserDataKey))},
			tableSortKey:      {S: aws.String(contextHashKey)},
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

func getStringListFromSet(value *dynamodb.AttributeValue) []string {
	if value == nil || value.SS == nil {
		return nil
	}
	ret := make([]string, len(value.SS))
	for i, ss := range value.SS {
		ret[i] = *ss
	}
	return ret
}

func (store *dynamoDBBigSegmentStoreImpl) Close() error {
	return nil
}

func prefixedNamespace(prefix, baseNamespace string) string {
	if prefix == "" {
		return baseNamespace
	}
	return prefix + ":" + baseNamespace // COVERAGE: currently the standard test suite doesn't specify a prefix
}
