package lddynamodb

import (
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/subsystems"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// StoreBuilder is a builder for configuring the DynamoDB-based persistent data store and/or Big
// Segment store.
//
// Both [DataStore] and [BigSegmentStore] return instances of this type. You can use methods of the
// builder to specify any ny non-default DynamoDB options you may want, before passing the builder to
// either [github.com/launchdarkly/go-server-sdk/v7/ldcomponents.PersistentDataStore] or
// [github.com/launchdarkly/go-server-sdk/v7/ldcomponents.BigSegments] as appropriate. The two types
// of stores are independent of each other; you do not need a Big Segment store if you are not using
// the Big Segments feature, and you do not need to use the same DynamoDB options for both.
//
// In this example, the main data store uses a DynamoDB table called "table1", and the Big Segment
// store uses a DynamoDB table called "table2":
//
//	config.DataStore = ldcomponents.PersistentDataStore(
//		lddynamodb.DataStore("table1"))
//	config.BigSegments = ldcomponents.BigSegments(
//		lddynamodb.BigSegmentStore("table2"))
//
// Note that the SDK also has its own options related to data storage that are configured
// at a different level, because they are independent of what database is being used. For
// instance, the builder returned by [github.com/launchdarkly/go-server-sdk/v7/ldcomponents.PersistentDataStore]
// has options for caching:
//
//	config.DataStore = ldcomponents.PersistentDataStore(
//		lddynamodb.DataStore("table1"),
//	).CacheSeconds(15)
type StoreBuilder[T any] struct {
	builderOptions
	factory func(*StoreBuilder[T], subsystems.ClientContext) (T, error)
}

type builderOptions struct {
	client        *dynamodb.Client
	table         string
	prefix        string
	awsConfig     *aws.Config
	clientOptions *dynamodb.Options
	clientOptFns  []func(*dynamodb.Options)
}

// DataStore returns a configurable builder for a DynamoDB-backed data store.
//
// This is for the main data store that holds feature flag data. To configure a data store for
// Big Segments, use [BigSegmentStore] instead.
//
// The tableName parameter is required, and the table must already exist in DynamoDB.
//
// You can use methods of the builder to specify any non-default DynamoDB options you may want,
// before passing the builder to [github.com/launchdarkly/go-server-sdk/v7/ldcomponents.PersistentDataStore].
// In this example, the store is configured to use a DynamoDB table called "table1" and the AWS
// region is forced to be "us-east-1":
//
//	config.DataStore = ldcomponents.PersistentDataStore(
//		lddynamodb.DataStore("table1").ClientOptions(dynamodb.Options{Region: "us-east-1"}),
//	)
//
// Note that the SDK also has its own options related to data storage that are configured
// at a different level, because they are independent of what database is being used. For
// instance, the builder returned by [github.com/launchdarkly/go-server-sdk/v7/ldcomponents.PersistentDataStore]
// has options for caching:
//
//	config.DataStore = ldcomponents.PersistentDataStore(
//		lddynamodb.DataStore("table1"),
//	).CacheSeconds(15)
func DataStore(tableName string) *StoreBuilder[subsystems.PersistentDataStore] {
	return &StoreBuilder[subsystems.PersistentDataStore]{
		builderOptions: builderOptions{
			table: tableName,
		},
		factory: createPersistentDataStore,
	}
}

// BigSegmentStore returns a configurable builder for a DynamoDB-backed Big Segment store.
//
// The tableName parameter is required, and the table must already exist in DynamoDB.
//
// You can use methods of the builder to specify any non-default Redis options you may want,
// before passing the builder to [github.com/launchdarkly/go-server-sdk/v7/ldcomponents.BigSegments].
// In this example, the store is configured to use a DynamoDB table called "table1" and the AWS
// region is forced to be "us-east-1":
//
//	config.BigSegments = ldcomponents.BigSegments(
//		lddynamodb.BigSegmentStore("table1").ClientOptions(dynamodb.Options{Region: "us-east-1"}),
//	)
//
// Note that the SDK also has its own options related to Big Segments that are configured
// at a different level, because they are independent of what database is being used. For
// instance, the builder returned by [github.com/launchdarkly/go-server-sdk/v7/ldcomponents.BigSegments]
// has an option for the status polling interval:
//
//	config.BigSegments = ldcomponents.BigSegments(
//		lddynamodb.BigSegmentStore("table1"),
//	).StatusPollInterval(time.Second * 30)
func BigSegmentStore(tableName string) *StoreBuilder[subsystems.BigSegmentStore] {
	return &StoreBuilder[subsystems.BigSegmentStore]{
		builderOptions: builderOptions{
			table: tableName,
		},
		factory: createBigSegmentStore,
	}
}

// Prefix specifies a prefix for namespacing the data store's keys.
func (b *StoreBuilder[T]) Prefix(prefix string) *StoreBuilder[T] {
	b.prefix = prefix
	return b
}

// DynamoClient specifies an existing DynamoDB client instance. Use this if you want to customize the client
// used by the data store in ways that are not supported by other DataStoreBuilder options. If you
// specify this option, then any configurations specified with SessionOptions or ClientConfig will be ignored.
func (b *StoreBuilder[T]) DynamoClient(client *dynamodb.Client) *StoreBuilder[T] {
	b.client = client
	return b
}

// ClientOptions specifies custom parameters for the dynamodb.NewFromConfig client constructor. This can be used
// to set properties such as the region programmatically, rather than relying on the defaults from the environment.
func (b *StoreBuilder[T]) ClientConfig(options aws.Config, optFns ...func(*dynamodb.Options)) *StoreBuilder[T] {
	b.awsConfig = &options
	b.clientOptFns = optFns
	return b
}

// ClientOptions specifies custom parameters for the dynamodb.New client constructor. This can be used to set
// properties such as the region programmatically, rather than relying on the defaults from the environment.
func (b *StoreBuilder[T]) ClientOptions(options dynamodb.Options, optFns ...func(*dynamodb.Options)) *StoreBuilder[T] {
	b.awsConfig = nil
	b.clientOptions = &options
	b.clientOptFns = optFns
	return b
}

// Build is called internally by the SDK.
func (b *StoreBuilder[T]) Build(context subsystems.ClientContext) (T, error) {
	return b.factory(b, context)
}

// DescribeConfiguration is used internally by the SDK to inspect the configuration.
func (b *StoreBuilder[T]) DescribeConfiguration() ldvalue.Value {
	return ldvalue.String("DynamoDB")
}

func createPersistentDataStore(
	builder *StoreBuilder[subsystems.PersistentDataStore],
	clientContext subsystems.ClientContext,
) (subsystems.PersistentDataStore, error) {
	return newDynamoDBDataStoreImpl(builder.builderOptions, clientContext.GetLogging().Loggers)
}

func createBigSegmentStore(
	builder *StoreBuilder[subsystems.BigSegmentStore],
	clientContext subsystems.ClientContext,
) (subsystems.BigSegmentStore, error) {
	return newDynamoDBBigSegmentStoreImpl(builder.builderOptions, clientContext.GetLogging().Loggers)
}
