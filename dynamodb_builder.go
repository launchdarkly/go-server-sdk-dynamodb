package lddynamodb

import (
	"gopkg.in/launchdarkly/go-sdk-common.v2/ldvalue"
	"gopkg.in/launchdarkly/go-server-sdk.v5/interfaces"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DataStoreBuilder is a builder for configuring the DynamoDB-based persistent data store.
//
// This can be used either for the main data store that holds feature flag data, or for the big
// segment store, or both. If you are using both, they do not have to have the same parameters. For
// instance, in this example the main data store uses the table "table1" and the big segment store
// uses the table "table2":
//
//     config.DataStore = ldcomponents.PersistentDataStore(
//         lddynamodb.DataStore("table1"))
//     config.BigSegments = ldcomponents.BigSegments(
//         lddynamodb.DataStore("table2"))
//
// Note that the builder is passed to one of two methods, PersistentDataStore or BigSegments,
// depending on the context in which it is being used. This is because each of those contexts has
// its own additional configuration options that are unrelated to the DynamoDB options.
//
// Builder calls can be chained, for example:
//
//     config.DataStore = lddynamodb.DataStore("tablename").SessionOptions(someOption).Prefix("prefix")
//
// You do not need to call the builder's CreatePersistentDataStore() method yourself to build the
// actual data store; that will be done by the SDK.
type DataStoreBuilder struct {
	client        *dynamodb.Client
	table         string
	prefix        string
	awsConfig     *aws.Config
	clientOptions *dynamodb.Options
	clientOptFns  []func(*dynamodb.Options)
}

// DataStore returns a configurable builder for a DynamoDB-backed data store.
//
// The tableName parameter is required, and the table must already exist in DynamoDB.
func DataStore(tableName string) *DataStoreBuilder {
	return &DataStoreBuilder{
		table: tableName,
	}
}

// Prefix specifies a prefix for namespacing the data store's keys.
func (b *DataStoreBuilder) Prefix(prefix string) *DataStoreBuilder {
	b.prefix = prefix
	return b
}

// DynamoClient specifies an existing DynamoDB client instance. Use this if you want to customize the client
// used by the data store in ways that are not supported by other DataStoreBuilder options. If you
// specify this option, then any configurations specified with SessionOptions or ClientConfig will be ignored.
func (b *DataStoreBuilder) DynamoClient(client *dynamodb.Client) *DataStoreBuilder {
	b.client = client
	return b
}

// ClientOptions specifies custom parameters for the dynamodb.NewFromConfig client constructor. This can be used
// to set properties such as the region programmatically, rather than relying on the defaults from the environment.
func (b *DataStoreBuilder) ClientConfig(options aws.Config, optFns ...func(*dynamodb.Options)) *DataStoreBuilder {
	b.awsConfig = &options
	b.clientOptFns = optFns
	return b
}

// ClientOptions specifies custom parameters for the dynamodb.New client constructor. This can be used to set
// properties such as the region programmatically, rather than relying on the defaults from the environment.
func (b *DataStoreBuilder) ClientOptions(options dynamodb.Options, optFns ...func(*dynamodb.Options)) *DataStoreBuilder {
	b.awsConfig = nil
	b.clientOptions = &options
	b.clientOptFns = optFns
	return b
}

// CreatePersistentDataStore is called internally by the SDK to create a data store implementation object.
func (b *DataStoreBuilder) CreatePersistentDataStore(
	context interfaces.ClientContext,
) (interfaces.PersistentDataStore, error) {
	store, err := newDynamoDBDataStoreImpl(b, context.GetLogging().GetLoggers())
	return store, err
}

// CreateBigSegmentStore is called internally by the SDK to create a data store implementation object.
func (b *DataStoreBuilder) CreateBigSegmentStore(
	context interfaces.ClientContext,
) (interfaces.BigSegmentStore, error) {
	store, err := newDynamoDBBigSegmentStoreImpl(b, context.GetLogging().GetLoggers())
	if err != nil {
		return nil, err
	}
	return store, err
}

// DescribeConfiguration is used internally by the SDK to inspect the configuration.
func (b *DataStoreBuilder) DescribeConfiguration() ldvalue.Value {
	return ldvalue.String("DynamoDB")
}
