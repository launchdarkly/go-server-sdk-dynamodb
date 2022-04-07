package lddynamodb

import (
	"fmt"
	"strconv"
	"testing"

	"gopkg.in/launchdarkly/go-server-sdk.v5/interfaces"
	"gopkg.in/launchdarkly/go-server-sdk.v5/testhelpers/storetest"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
)

func TestBigSegmentStore(t *testing.T) {
	err := createTableIfNecessary()
	require.NoError(t, err)

	client, err := createTestClient()
	require.NoError(t, err)

	setTestMetadata := func(prefix string, metadata interfaces.BigSegmentStoreMetadata) error {
		key := prefixedNamespace(prefix, bigSegmentsMetadataKey)
		item := map[string]*dynamodb.AttributeValue{
			tablePartitionKey:       {S: aws.String(key)},
			tableSortKey:            {S: aws.String(key)},
			bigSegmentsSyncTimeAttr: {N: aws.String(strconv.Itoa(int(metadata.LastUpToDate)))},
		}
		_, err := client.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(testTableName),
			Item:      item,
		})
		return err
	}

	addToSet := func(prefix, userHashKey, attrName, value string) error {
		_, err := client.UpdateItem(&dynamodb.UpdateItemInput{
			TableName: aws.String(testTableName),
			Key: map[string]*dynamodb.AttributeValue{
				tablePartitionKey: {S: aws.String(prefixedNamespace(prefix, bigSegmentsUserDataKey))},
				tableSortKey:      {S: aws.String(userHashKey)},
			},
			UpdateExpression: aws.String(fmt.Sprintf("ADD %s :value", attrName)),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":value": {SS: []*string{aws.String(value)}},
			},
		})
		return err
	}
	setTestSegments := func(prefix string, userHashKey string, included []string, excluded []string) error {
		for _, inc := range included {
			if err := addToSet(prefix, userHashKey, "included", inc); err != nil {
				return err
			}
		}
		for _, exc := range excluded {
			if err := addToSet(prefix, userHashKey, "excluded", exc); err != nil {
				return err
			}
		}
		return nil
	}

	storetest.NewBigSegmentStoreTestSuite(
		func(prefix string) interfaces.BigSegmentStoreFactory {
			return baseBuilder().Prefix(prefix)
		},
		clearTestData,
		setTestMetadata,
		setTestSegments,
	).Run(t)
}
