package lddynamodb

// This is based on code from https://github.com/mlafeldt/launchdarkly-dynamo-store.
// Changes include a different method of configuration, a different method of marshaling
// objects, less potential for race conditions, and unit tests that run against a local
// Dynamo instance.

// Implementation notes:
//
// - Feature flags, segments, and any other kind of entity the LaunchDarkly client may wish
// to store, are all put in the same table. The only two required attributes are "key" (which
// is present in all storeable entities) and "namespace" (a parameter from the client that is
// used to disambiguate between flags and segments).
//
// - Because of DynamoDB's restrictions on attribute values (e.g. empty strings are not
// allowed), the standard DynamoDB marshaling mechanism with one attribute per object property
// is not used. Instead, the entire object is serialized to JSON and stored in a single
// attribute, "item". The "version" property is also stored as a separate attribute since it
// is used for updates.
//
// - Since DynamoDB doesn't have transactions, the Init method - which replaces the entire data
// store - is not atomic, so there can be a race condition if another process is adding new data
// via Upsert. To minimize this, we don't delete all the data at the start; instead, we update
// the items we've received, and then delete all other items. That could potentially result in
// deleting new data from another process, but that would be the case anyway if the Init
// happened to execute later than the Upsert; we are relying on the fact that normally the
// process that did the Init will also receive the new data shortly and do its own Upsert.
//
// - DynamoDB has a maximum item size of 400KB. Since each feature flag or user segment is
// stored as a single item, this mechanism will not work for extremely large flags or segments.

import (
	"context"
	"errors"
	"fmt"

	"gopkg.in/launchdarkly/go-sdk-common.v2/ldlog"
	"gopkg.in/launchdarkly/go-server-sdk.v5/interfaces/ldstoretypes"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	// Schema of the DynamoDB table
	tablePartitionKey = "namespace"
	tableSortKey      = "key"
	versionAttribute  = "version"
	itemJSONAttribute = "item"

	// We won't try to store items whose total size exceeds this. The DynamoDB documentation says
	// only "400KB", which probably means 400*1024, but to avoid any chance of trying to store a
	// too-large item we are rounding it down.
	dynamoDbMaxItemSize = 400000
)

type namespaceAndKey struct {
	namespace string
	key       string
}

// Internal type for our DynamoDB implementation of the ld.DataStore interface.
type dynamoDBDataStore struct {
	client         *dynamodb.Client
	context        context.Context
	cancelContext  func()
	table          string
	prefix         string
	loggers        ldlog.Loggers
	testUpdateHook func() // Used only by unit tests - see updateWithVersioning
}

func newDynamoDBDataStoreImpl(builder *DataStoreBuilder, loggers ldlog.Loggers) (*dynamoDBDataStore, error) {
	if builder.table == "" {
		return nil, errors.New("table name is required")
	}

	client, context, cancelContext, err := makeClientAndContext(builder)
	if err != nil {
		return nil, err
	}
	store := &dynamoDBDataStore{
		client:        client,
		context:       context,
		cancelContext: cancelContext,
		table:         builder.table,
		prefix:        builder.prefix,
		loggers:       loggers, // copied by value so we can modify it
	}
	store.loggers.SetPrefix("DynamoDBDataStore:")
	store.loggers.Infof(`Using DynamoDB table %s`, store.table)

	return store, nil
}

func (store *dynamoDBDataStore) Init(allData []ldstoretypes.SerializedCollection) error {
	// Start by reading the existing keys; we will later delete any of these that weren't in allData.
	unusedOldKeys, err := store.readExistingKeys(allData)
	if err != nil {
		return fmt.Errorf("failed to get existing items prior to Init: %s", err)
	}

	requests := make([]types.WriteRequest, 0)
	numItems := 0

	// Insert or update every provided item
	for _, coll := range allData {
		for _, item := range coll.Items {
			av := store.encodeItem(coll.Kind, item.Key, item.Item)
			if !store.checkSizeLimit(av) {
				continue
			}
			requests = append(requests, types.WriteRequest{
				PutRequest: &types.PutRequest{Item: av},
			})
			nk := namespaceAndKey{namespace: store.namespaceForKind(coll.Kind), key: item.Key}
			unusedOldKeys[nk] = false
			numItems++
		}
	}

	// Now delete any previously existing items whose keys were not in the current data
	initedKey := store.initedKey()
	for k, v := range unusedOldKeys {
		if v && k.namespace != initedKey {
			delKey := map[string]types.AttributeValue{
				tablePartitionKey: attrValueOfString(k.namespace),
				tableSortKey:      attrValueOfString(k.key),
			}
			requests = append(requests, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{Key: delKey},
			})
		}
	}

	// Now set the special key that we check in InitializedInternal()
	initedItem := map[string]types.AttributeValue{
		tablePartitionKey: attrValueOfString(initedKey),
		tableSortKey:      attrValueOfString(initedKey),
	}
	requests = append(requests, types.WriteRequest{
		PutRequest: &types.PutRequest{Item: initedItem},
	})

	if err := batchWriteRequests(store.context, store.client, store.table, requests); err != nil {
		// COVERAGE: can't cause an error here in unit tests because we only get this far if the
		// DynamoDB client is successful on the initial query
		return fmt.Errorf("failed to write %d items(s) in batches: %s", len(requests), err)
	}

	store.loggers.Infof("Initialized table %q with %d item(s)", store.table, numItems)

	return nil
}

func (store *dynamoDBDataStore) IsInitialized() bool {
	result, err := store.client.GetItem(store.context, &dynamodb.GetItemInput{
		TableName:      aws.String(store.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			tablePartitionKey: attrValueOfString(store.initedKey()),
			tableSortKey:      attrValueOfString(store.initedKey()),
		},
	})
	return err == nil && len(result.Item) != 0
}

func (store *dynamoDBDataStore) GetAll(
	kind ldstoretypes.DataKind,
) ([]ldstoretypes.KeyedSerializedItemDescriptor, error) {
	var results []ldstoretypes.KeyedSerializedItemDescriptor
	for paginator := dynamodb.NewQueryPaginator(store.client, store.makeQueryForKind(kind)); paginator.HasMorePages(); {
		out, err := paginator.NextPage(store.context)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			if key, serializedItemDesc, ok := store.decodeItem(item); ok {
				results = append(results, ldstoretypes.KeyedSerializedItemDescriptor{
					Key:  key,
					Item: serializedItemDesc,
				})
			}
		}
	}
	return results, nil
}

func (store *dynamoDBDataStore) Get(
	kind ldstoretypes.DataKind,
	key string,
) (ldstoretypes.SerializedItemDescriptor, error) {
	result, err := store.client.GetItem(store.context, &dynamodb.GetItemInput{
		TableName:      aws.String(store.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			tablePartitionKey: attrValueOfString(store.namespaceForKind(kind)),
			tableSortKey:      attrValueOfString(key),
		},
	})
	if err != nil {
		return ldstoretypes.SerializedItemDescriptor{}.NotFound(),
			fmt.Errorf("failed to get %s key %s: %s", kind, key, err)
	}

	if len(result.Item) == 0 {
		if store.loggers.IsDebugEnabled() { // COVERAGE: tests don't verify debug logging
			store.loggers.Debugf("Item not found (key=%s)", key)
		}
		return ldstoretypes.SerializedItemDescriptor{}.NotFound(), nil
	}

	if _, serializedItemDesc, ok := store.decodeItem(result.Item); ok {
		return serializedItemDesc, nil
	}
	return ldstoretypes.SerializedItemDescriptor{}.NotFound(), // COVERAGE: can't cause this in unit tests
		fmt.Errorf("invalid data for %s key %s: %s", kind, key, err)
}

func (store *dynamoDBDataStore) Upsert(
	kind ldstoretypes.DataKind,
	key string,
	newItem ldstoretypes.SerializedItemDescriptor,
) (bool, error) {
	av := store.encodeItem(kind, key, newItem)
	if !store.checkSizeLimit(av) {
		return false, nil
	}

	if store.testUpdateHook != nil {
		store.testUpdateHook()
	}

	_, err := store.client.PutItem(store.context, &dynamodb.PutItemInput{
		TableName: aws.String(store.table),
		Item:      av,
		ConditionExpression: aws.String(
			"attribute_not_exists(#namespace) or " +
				"attribute_not_exists(#key) or " +
				":version > #version",
		),
		ExpressionAttributeNames: map[string]string{
			"#namespace": tablePartitionKey,
			"#key":       tableSortKey,
			"#version":   versionAttribute,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":version": attrValueOfInt(newItem.Version),
		},
	})
	if err != nil {
		var condCheckErr *types.ConditionalCheckFailedException
		if errors.As(err, &condCheckErr) {
			if store.loggers.IsDebugEnabled() { // COVERAGE: tests don't verify debug logging
				store.loggers.Debugf("Not updating item due to condition (namespace=%s key=%s version=%d)",
					kind, key, newItem.Version)
			}
			return false, nil
		}
		return false, fmt.Errorf("failed to put %s key %s: %s", kind, key, err)
	}

	return true, nil
}

func (store *dynamoDBDataStore) IsStoreAvailable() bool {
	// There doesn't seem to be a specific DynamoDB API for just testing the connection. We will just
	// do a simple query for the "inited" key, and test whether we get an error ("not found" does not
	// count as an error).
	_, err := store.client.GetItem(store.context, &dynamodb.GetItemInput{
		TableName:      aws.String(store.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			tablePartitionKey: attrValueOfString(store.initedKey()),
			tableSortKey:      attrValueOfString(store.initedKey()),
		},
	})
	return err == nil
}

func (store *dynamoDBDataStore) Close() error {
	store.cancelContext() // stops any pending operations
	return nil
}

func (store *dynamoDBDataStore) prefixedNamespace(baseNamespace string) string {
	if store.prefix == "" {
		return baseNamespace
	}
	return store.prefix + ":" + baseNamespace
}

func (store *dynamoDBDataStore) namespaceForKind(kind ldstoretypes.DataKind) string {
	return store.prefixedNamespace(kind.GetName())
}

func (store *dynamoDBDataStore) initedKey() string {
	return store.prefixedNamespace("$inited")
}

func (store *dynamoDBDataStore) makeQueryForKind(kind ldstoretypes.DataKind) *dynamodb.QueryInput {
	return &dynamodb.QueryInput{
		TableName:      aws.String(store.table),
		ConsistentRead: aws.Bool(true),
		KeyConditions: map[string]types.Condition{
			tablePartitionKey: {
				ComparisonOperator: types.ComparisonOperatorEq,
				AttributeValueList: []types.AttributeValue{
					attrValueOfString(store.namespaceForKind(kind)),
				},
			},
		},
	}
}

func (store *dynamoDBDataStore) readExistingKeys(
	newData []ldstoretypes.SerializedCollection,
) (map[namespaceAndKey]bool, error) {
	keys := make(map[namespaceAndKey]bool)
	for _, coll := range newData {
		kind := coll.Kind
		query := store.makeQueryForKind(kind)
		query.ProjectionExpression = aws.String("#namespace, #key")
		query.ExpressionAttributeNames = map[string]string{
			"#namespace": tablePartitionKey,
			"#key":       tableSortKey,
		}
		for paginator := dynamodb.NewQueryPaginator(store.client, store.makeQueryForKind(kind)); paginator.HasMorePages(); {
			out, err := paginator.NextPage(store.context)
			if err != nil {
				return nil, err
			}
			for _, i := range out.Items {
				nk := namespaceAndKey{namespace: attrValueToString(i[tablePartitionKey]),
					key: attrValueToString(i[tableSortKey])}
				keys[nk] = true
			}
		}
	}
	return keys, nil
}

func (store *dynamoDBDataStore) decodeItem(
	av map[string]types.AttributeValue,
) (string, ldstoretypes.SerializedItemDescriptor, bool) {
	key := attrValueToString(av[tableSortKey])
	version := attrValueToInt(av[versionAttribute])
	itemJSON := attrValueToString(av[itemJSONAttribute])
	if key != "" {
		return key, ldstoretypes.SerializedItemDescriptor{
			Version:        version,
			SerializedItem: []byte(itemJSON),
		}, true
	}
	return "", ldstoretypes.SerializedItemDescriptor{}, false // COVERAGE: no way to cause this in unit tests
}

func (store *dynamoDBDataStore) encodeItem(
	kind ldstoretypes.DataKind,
	key string,
	item ldstoretypes.SerializedItemDescriptor,
) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		tablePartitionKey: attrValueOfString(store.namespaceForKind(kind)),
		tableSortKey:      attrValueOfString(key),
		versionAttribute:  attrValueOfInt(item.Version),
		itemJSONAttribute: attrValueOfString(string(item.SerializedItem)),
	}
}

func (store *dynamoDBDataStore) checkSizeLimit(item map[string]types.AttributeValue) bool {
	// see: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
	size := 100 // fixed overhead for index data
	for key, value := range item {
		size += len(key) + len(attrValueToString(value))
	}
	if size <= dynamoDbMaxItemSize {
		return true
	}
	store.loggers.Errorf("The item %q in %q was too large to store in DynamoDB and was dropped",
		attrValueToString(item[tablePartitionKey]), attrValueToString(item[tableSortKey]))
	return false
}
