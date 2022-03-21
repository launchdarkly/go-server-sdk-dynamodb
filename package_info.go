// Package lddynamodb provides a DynamoDB-backed persistent data store for the LaunchDarkly Go SDK.
//
// For more details about how and why you can use a persistent data store, see:
// https://docs.launchdarkly.com/sdk/features/storing-data/dynamodb#go
//
// To use the DynamoDB data store with the LaunchDarkly client:
//
//     import lddynamodb "github.com/launchdarkly/go-server-sdk-dynamodb/v2"
//
//     config := ld.Config{
//         DataStore: ldcomponents.PersistentDataStore(lddynamodb.DataStore("my-table-name")),
//     }
//     client, err := ld.MakeCustomClient("sdk-key", config, 5*time.Second)
//
// By default, the data store uses a basic DynamoDB client configuration that is
// equivalent to doing this:
//
//     dynamoClient := dynamodb.New(session.NewSession())
//
// This default configuration will only work if your AWS credentials and region are
// available from AWS environment variables and/or configuration files. If you want to
// set those programmatically or modify any other configuration settings, you can use the
// methods of the lddynamodb.DataStoreBuilder returned by lddynamodb.DataStore(). For example:
//
//     config := ld.Config{
//         DataStore: ldcomponents.PersistentDataStore(
//             lddynamodb.DataStore("my-table-name").Prefix("key-prefix"),
//         ).CacheSeconds(30),
//     }
//
// Note that CacheSeconds() is not a method of lddynamodb.DataStoreBuilder, but rather a method of
// ldcomponents.PersistentDataStore(), because the caching behavior is provided by the SDK for
// all database integrations.
//
// If you are also using DynamoDB for other purposes, the data store can coexist with
// other data in the same table as long as you use the Prefix option to make each application
// use different keys. However, it is advisable to configure separate tables in DynamoDB, for
// better control over permissions and throughput.
package lddynamodb
