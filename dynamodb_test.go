package lddynamodb

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-sdk-common/v3/ldlogtest"
	"github.com/launchdarkly/go-server-sdk-evaluation/v2/ldbuilders"
	"github.com/launchdarkly/go-server-sdk/v6/interfaces"
	"github.com/launchdarkly/go-server-sdk/v6/interfaces/ldstoretypes"
	"github.com/launchdarkly/go-server-sdk/v6/ldcomponents"
	"github.com/launchdarkly/go-server-sdk/v6/ldcomponents/ldstoreimpl"
	"github.com/launchdarkly/go-server-sdk/v6/testhelpers"
	"github.com/launchdarkly/go-server-sdk/v6/testhelpers/storetest"
	"github.com/launchdarkly/go-test-helpers/v2/jsonhelpers"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
	return DataStore(testTableName).SessionOptions(makeTestOptions())
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
	assert.Contains(t, err.Error(), "could not find region configuration")
}

func clearTestData(prefix string) error {
	if prefix != "" {
		prefix += ":"
	}

	client, err := createTestClient()
	if err != nil {
		return err
	}
	var items []map[string]*dynamodb.AttributeValue

	err = client.ScanPages(&dynamodb.ScanInput{
		TableName:            aws.String(testTableName),
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("#namespace, #key"),
		ExpressionAttributeNames: map[string]*string{
			"#namespace": aws.String(tablePartitionKey),
			"#key":       aws.String(tableSortKey),
		},
	}, func(out *dynamodb.ScanOutput, lastPage bool) bool {
		items = append(items, out.Items...)
		return !lastPage
	})
	if err != nil {
		return err
	}

	var requests []*dynamodb.WriteRequest
	for _, item := range items {
		if strings.HasPrefix(*item[tablePartitionKey].S, prefix) {
			requests = append(requests, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{Key: item},
			})
		}
	}
	return batchWriteRequests(client, testTableName, requests)
}

func setConcurrentModificationHook(store interfaces.PersistentDataStore, hook func()) {
	store.(*dynamoDBDataStore).testUpdateHook = hook
}

func createTestClient() (*dynamodb.DynamoDB, error) {
	sess, err := session.NewSessionWithOptions(makeTestOptions())
	if err != nil {
		return nil, err
	}
	return dynamodb.New(sess), nil
}

func makeTestOptions() session.Options {
	return session.Options{
		Config: aws.Config{
			Credentials: credentials.NewStaticCredentials("dummy", "not", "used"),
			Endpoint:    aws.String(localEndpoint),
			Region:      aws.String("us-east-1"), // this is ignored for a local instance, but is still required
		},
	}
}

func createTableIfNecessary() error {
	client, err := createTestClient()
	if err != nil {
		return err
	}
	_, err = client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(testTableName)})
	if err == nil {
		return nil
	}
	if e, ok := err.(awserr.Error); !ok || e.Code() != dynamodb.ErrCodeResourceNotFoundException {
		return err
	}
	createParams := dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(tablePartitionKey),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String(tableSortKey),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(tablePartitionKey),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String(tableSortKey),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(testTableName),
	}
	_, err = client.CreateTable(&createParams)
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
			tableInfo, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(testTableName)})
			if err == nil && *tableInfo.Table.TableStatus == dynamodb.TableStatusActive {
				return nil
			}
		}
	}
}
