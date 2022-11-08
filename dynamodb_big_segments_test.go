package lddynamodb

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/launchdarkly/go-server-sdk/v6/subsystems"
	"github.com/launchdarkly/go-server-sdk/v6/testhelpers/storetest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestBigSegmentStore(t *testing.T) {
	err := createTableIfNecessary()
	require.NoError(t, err)

	client := createTestClient()

	setTestMetadata := func(prefix string, metadata subsystems.BigSegmentStoreMetadata) error {
		key := prefixedNamespace(prefix, bigSegmentsMetadataKey)
		item := map[string]types.AttributeValue{
			tablePartitionKey:       attrValueOfString(key),
			tableSortKey:            attrValueOfString(key),
			bigSegmentsSyncTimeAttr: &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(metadata.LastUpToDate), 10)},
		}
		_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String(testTableName),
			Item:      item,
		})
		return err
	}

	addToSet := func(prefix, contextHashKey, attrName, value string) error {
		_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
			TableName: aws.String(testTableName),
			Key: map[string]types.AttributeValue{
				tablePartitionKey: attrValueOfString(prefixedNamespace(prefix, bigSegmentsUserDataKey)),
				tableSortKey:      attrValueOfString(contextHashKey),
			},
			UpdateExpression: aws.String(fmt.Sprintf("ADD %s :value", attrName)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":value": &types.AttributeValueMemberSS{Value: []string{value}},
			},
		})
		return err
	}
	setTestSegments := func(prefix string, contextHashKey string, included []string, excluded []string) error {
		for _, inc := range included {
			if err := addToSet(prefix, contextHashKey, "included", inc); err != nil {
				return err
			}
		}
		for _, exc := range excluded {
			if err := addToSet(prefix, contextHashKey, "excluded", exc); err != nil {
				return err
			}
		}
		return nil
	}

	storetest.NewBigSegmentStoreTestSuite(
		func(prefix string) subsystems.ComponentConfigurer[subsystems.BigSegmentStore] {
			return baseBigSegmentStoreBuilder().Prefix(prefix)
		},
		clearTestData,
		setTestMetadata,
		setTestSegments,
	).Run(t)
}

func baseBigSegmentStoreBuilder() *StoreBuilder[subsystems.BigSegmentStore] {
	return BigSegmentStore(testTableName).ClientOptions(makeTestOptions())
}
