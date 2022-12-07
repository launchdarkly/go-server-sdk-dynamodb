# Change log

All notable changes to the LaunchDarkly Go SDK DynamoDB integration will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org).

## [3.0.1] - 2022-12-07
### Fixed:
- Updated SDK dependency to use v6.0.0 release.

## [3.0.0] - 2022-12-07
This release corresponds to the 6.0.0 release of the LaunchDarkly Go SDK. Any application code that is being updated to use the 6.0.0 SDK, and was using a 2.x version of `go-server-sdk-dynamodb`, should now use a 3.x version instead.

There are no functional differences in the behavior of the DynamoDB integration; the differences are only related to changes in the usage of interface types for configuration in the SDK.

### Added:
- `BigSegmentStore()`, which creates a configuration builder for use with Big Segments. Previously, the `DataStore()` builder was used for both regular data stores and Big Segment stores.

### Changed:
- The type `DynamoDBDataStoreBuilder` has been removed, replaced by a generic type `DynamoDBStoreBuilder`. Application code would not normally need to reference these types by name, but if necessary, use either `DynamoDBStoreBuilder[PersistentDataStore]` or `DynamoDBStoreBuilder[BigSegmentStore]` depending on whether you are configuring a regular data store or a Big Segment store.

## [2.0.0] - 2022-09-07
This release updates the integration to use [`aws-sdk-go-v2`](https://github.com/aws/aws-sdk-go-v2) instead of the older AWS SDK. There is no functional difference in terms of database operations.

For applications that already use the v2 AWS SDK for other purposes, updating to this version removes an extra dependency and allows application code to configure the integration using the v2 configuration types. For applications that do not use the AWS SDK themselves, we still recommend updating to this version because the older AWS SDK will not be maintained forever and has had security vulnerabilities reported.

### Added:
- `DataStoreBuilder.ClientConfig` and `DataStoreBuilder.ClientOptions`, which use the newer AWS SDK configuration types.

### Changed:
- `DataStoreBuilder.DynamoClient()` now takes a parameter of type `*dynamodb.Client`, since the `dynamodbiface.DynamoDBAPI` no longer exists.

### Removed:
- `DataStoreBuilder.SessionOptions`

## [1.1.1] - 2022-04-07
### Fixed:
- If the SDK attempts to store a feature flag or segment whose total data size is over the 400KB limit for DynamoDB items, this integration will now log (at `Error` level) a message like `The item "my-flag-key" in "features" was too large to store in DynamoDB and was dropped` but will still process all other data updates. Previously, it would cause the SDK to enter an error state in which the oversized item would be pointlessly retried and other updates might be lost.

## [1.1.0] - 2021-07-20
### Added:
- Added support for Big Segments. An Early Access Program for creating and syncing Big Segments from customer data platforms is available to enterprise customers.

## [1.0.1] - 2021-02-04
### Changed:
- Updated the default AWS SDK version to 1.37.2. Among other improvements as described in the [AWS Go SDK release notes](https://github.com/aws/aws-sdk-go/blob/master/CHANGELOG.md), this allows it to support [IAM roles for service accounts](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts-minimum-sdk.html).

## [1.0.0] - 2020-09-18
Initial release of the stand-alone version of this package to be used with versions 5.0.0 and above of the LaunchDarkly Server-Side SDK for Go.
