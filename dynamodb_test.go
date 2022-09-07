package lddynamodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/launchdarkly/go-test-helpers/v2/jsonhelpers"
	"gopkg.in/launchdarkly/go-sdk-common.v2/ldlog"
	"gopkg.in/launchdarkly/go-sdk-common.v2/ldlogtest"
	"gopkg.in/launchdarkly/go-server-sdk-evaluation.v1/ldbuilders"
	"gopkg.in/launchdarkly/go-server-sdk.v5/interfaces"
	"gopkg.in/launchdarkly/go-server-sdk.v5/interfaces/ldstoretypes"
	"gopkg.in/launchdarkly/go-server-sdk.v5/ldcomponents"
	"gopkg.in/launchdarkly/go-server-sdk.v5/ldcomponents/ldstoreimpl"
	"gopkg.in/launchdarkly/go-server-sdk.v5/testhelpers"
	"gopkg.in/launchdarkly/go-server-sdk.v5/testhelpers/storetest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTableName = "LD_DYNAMODB_TEST_TABLE"
	localEndpoint = "http://localhost:8000"
)

func TestDynamoDBDataStore(t *testing.T) {
	err := createTableIfNecessary()
	require.NoError(t, err)

	storetest.NewPersistentDataStoreTestSuite(makeTestStore, clearTestData).
		ErrorStoreFactory(makeFailedStore(), verifyFailedStoreError).
		ConcurrentModificationHook(setConcurrentModificationHook).
		Run(t)
}

func TestDataStoreSkipsAndLogsTooLargeItem(t *testing.T) {
	require.NoError(t, createTableIfNecessary())

	makeGoodData := func() []ldstoretypes.SerializedCollection {
		return []ldstoretypes.SerializedCollection{
			{
				Kind: ldstoreimpl.Features(),
				Items: []ldstoretypes.KeyedSerializedItemDescriptor{
					{
						Key: "flag1",
						Item: ldstoretypes.SerializedItemDescriptor{
							Version: 1, SerializedItem: []byte(`{"key": "flag1", "version": 1}`),
						},
					},
					{
						Key: "flag2",
						Item: ldstoretypes.SerializedItemDescriptor{
							Version: 1, SerializedItem: []byte(`{"key": "flag2", "version": 1}`),
						},
					},
				},
			},
			{
				Kind: ldstoreimpl.Segments(),
				Items: []ldstoretypes.KeyedSerializedItemDescriptor{
					{
						Key: "segment1",
						Item: ldstoretypes.SerializedItemDescriptor{
							Version: 1, SerializedItem: []byte(`{"key": "segment1", "version": 1}`),
						},
					},
					{
						Key: "segment2",
						Item: ldstoretypes.SerializedItemDescriptor{
							Version:        1,
							SerializedItem: []byte(`{"key": "segment2", "version": 1}`),
						},
					},
				},
			},
		}
	}

	makeBigKeyList := func() []string {
		var ret []string
		for i := 0; i < 40000; i++ {
			ret = append(ret, fmt.Sprintf("key%d", i))
		}
		require.Greater(t, len(jsonhelpers.ToJSON(ret)), 400*1024)
		return ret
	}

	badItemKey := "baditem"
	tooBigFlag := ldbuilders.NewFlagBuilder(badItemKey).Version(1).
		AddTarget(0, makeBigKeyList()...).Build()
	tooBigSegment := ldbuilders.NewSegmentBuilder(badItemKey).Version(1).
		Included(makeBigKeyList()...).Build()

	kindParams := []struct {
		name      string
		dataKind  ldstoretypes.DataKind
		collIndex int
		item      ldstoretypes.SerializedItemDescriptor
	}{
		{"flags", ldstoreimpl.Features(), 0, ldstoretypes.SerializedItemDescriptor{
			Version:        tooBigFlag.Version,
			SerializedItem: jsonhelpers.ToJSON(tooBigFlag),
		}},
		{"segments", ldstoreimpl.Segments(), 1, ldstoretypes.SerializedItemDescriptor{
			Version:        tooBigSegment.Version,
			SerializedItem: jsonhelpers.ToJSON(tooBigSegment),
		}},
	}

	getAllData := func(t *testing.T, store interfaces.PersistentDataStore) []ldstoretypes.SerializedCollection {
		flags, err := store.GetAll(ldstoreimpl.Features())
		require.NoError(t, err)
		segments, err := store.GetAll(ldstoreimpl.Segments())
		require.NoError(t, err)
		return []ldstoretypes.SerializedCollection{
			{Kind: ldstoreimpl.Features(), Items: flags},
			{Kind: ldstoreimpl.Segments(), Items: segments},
		}
	}

	t.Run("init", func(t *testing.T) {
		for _, params := range kindParams {
			t.Run(params.name, func(t *testing.T) {
				mockLog := ldlogtest.NewMockLog()
				ctx := testhelpers.NewSimpleClientContext("").WithLogging(ldcomponents.Logging().Loggers(mockLog.Loggers))
				store, err := makeTestStore("").CreatePersistentDataStore(ctx)
				require.NoError(t, err)
				defer store.Close()

				dataPlusBadItem := makeGoodData()
				collection := dataPlusBadItem[params.collIndex]
				collection.Items = append(
					// put the bad item first to prove that items after that one are still stored
					[]ldstoretypes.KeyedSerializedItemDescriptor{
						{Key: badItemKey, Item: params.item},
					},
					collection.Items...,
				)
				dataPlusBadItem[params.collIndex] = collection

				require.NoError(t, store.Init(dataPlusBadItem))

				mockLog.AssertMessageMatch(t, true, ldlog.Error, "was too large to store in DynamoDB and was dropped")

				assert.Equal(t, makeGoodData(), getAllData(t, store))
			})
		}
	})

	t.Run("upsert", func(t *testing.T) {
		for _, params := range kindParams {
			t.Run(params.name, func(t *testing.T) {
				mockLog := ldlogtest.NewMockLog()
				ctx := testhelpers.NewSimpleClientContext("").WithLogging(ldcomponents.Logging().Loggers(mockLog.Loggers))
				store, err := makeTestStore("").CreatePersistentDataStore(ctx)
				require.NoError(t, err)
				defer store.Close()

				goodData := makeGoodData()
				require.NoError(t, store.Init(goodData))

				updated, err := store.Upsert(params.dataKind, badItemKey, params.item)
				assert.False(t, updated)
				assert.NoError(t, err)
				mockLog.AssertMessageMatch(t, true, ldlog.Error, "was too large to store in DynamoDB and was dropped")

				assert.Equal(t, goodData, getAllData(t, store))
			})
		}
	})
}

func baseBuilder() *DataStoreBuilder {
	return DataStore(testTableName).ClientOptions(makeTestOptions())
}

func makeTestStore(prefix string) interfaces.PersistentDataStoreFactory {
	return baseBuilder().Prefix(prefix)
}

func makeFailedStore() interfaces.PersistentDataStoreFactory {
	// Here we ensure that all DynamoDB operations will fail by simply *not* using makeTestOptions(),
	// so that the client does not have the necessary region parameter.
	return DataStore(testTableName)
}

func verifyFailedStoreError(t assert.TestingT, err error) {
	assert.Contains(t, err.Error(), "an AWS region is required")
}

func clearTestData(prefix string) error {
	if prefix != "" {
		prefix += ":"
	}

	client := createTestClient()
	var items []map[string]types.AttributeValue

	scanInput := dynamodb.ScanInput{
		TableName:            aws.String(testTableName),
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("#namespace, #key"),
		ExpressionAttributeNames: map[string]string{
			"#namespace": tablePartitionKey,
			"#key":       tableSortKey,
		},
	}
	for {
		out, err := client.Scan(context.Background(), &scanInput)
		if err != nil {
			return err
		}
		items = append(items, out.Items...)
		if out.LastEvaluatedKey == nil {
			break
		}
		scanInput.ExclusiveStartKey = out.LastEvaluatedKey
	}

	var requests []types.WriteRequest
	for _, item := range items {
		if strings.HasPrefix(attrValueToString(item[tablePartitionKey]), prefix) {
			requests = append(requests, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{Key: item},
			})
		}
	}
	return batchWriteRequests(context.Background(), client, testTableName, requests)
}

func setConcurrentModificationHook(store interfaces.PersistentDataStore, hook func()) {
	store.(*dynamoDBDataStore).testUpdateHook = hook
}

func createTestClient() *dynamodb.Client {
	return dynamodb.New(makeTestOptions())
}

func makeTestOptions() dynamodb.Options {
	return dynamodb.Options{
		Credentials:      credentials.NewStaticCredentialsProvider("dummy", "not", "used"),
		EndpointResolver: dynamodb.EndpointResolverFromURL(localEndpoint),
		Region:           "us-east-1", // this is ignored for a local instance, but is still required
	}
}

func createTableIfNecessary() error {
	client := createTestClient()
	_, err := client.DescribeTable(context.Background(),
		&dynamodb.DescribeTableInput{TableName: aws.String(testTableName)})
	if err == nil {
		return nil
	}
	var resNotFoundErr *types.ResourceNotFoundException
	if !errors.As(err, &resNotFoundErr) {
		return err
	}
	createParams := dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(tablePartitionKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(tableSortKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(tablePartitionKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(tableSortKey),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(testTableName),
	}
	_, err = client.CreateTable(context.Background(), &createParams)
	if err != nil {
		return err
	}
	// When DynamoDB creates a table, it may not be ready to use immediately
	deadline := time.After(10 * time.Second)
	retry := time.NewTicker(100 * time.Millisecond)
	defer retry.Stop()
	for {
		select {
		case <-deadline:
			return fmt.Errorf("timed out waiting for new table to be ready")
		case <-retry.C:
			tableInfo, err := client.DescribeTable(context.Background(),
				&dynamodb.DescribeTableInput{TableName: aws.String(testTableName)})
			if err == nil && tableInfo.Table.TableStatus == types.TableStatusActive {
				return nil
			}
		}
	}
}
