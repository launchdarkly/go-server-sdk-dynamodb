# LaunchDarkly Server-side SDK for Go - Dynamodb integration

[![Circle CI](https://circleci.com/gh/launchdarkly/go-server-sdk-dynamodb.svg?style=shield)](https://circleci.com/gh/launchdarkly/go-server-sdk-dynamodb) [![Documentation](https://img.shields.io/static/v1?label=go.dev&message=reference&color=00add8)](https://pkg.go.dev/github.com/launchdarkly/go-server-sdk-dynamodb/v3)

This library provides a [DynamoDB](https://aws.amazon.com/dynamodb/)-backed persistence mechanism (data store) for the [LaunchDarkly Go SDK](https://github.com/launchdarkly/go-server-sdk), replacing the default in-memory data store.

This version of the library requires at least version 6.0.0 of the LaunchDarkly Go SDK; for versions of the library to use with earlier SDK versions, see the changelog.

This version of the library uses the [v2 AWS Go SDK](https://github.com/aws/aws-sdk-go-v2).

The minimum Go version is 1.18.

For more information, see also: [Using DynamoDB as a persistent feature store](https://docs.launchdarkly.com/sdk/features/storing-data/dynamodb#go).

## Quick setup

This assumes that you have already installed the LaunchDarkly Go SDK.

1. Import the LaunchDarkly SDK packages and the package for this library:

```go
import (
    ld "github.com/launchdarkly/go-server-sdk/v6"
    "github.com/launchdarkly/go-server-sdk/v6/ldcomponents"
    lddynamodb "github.com/launchdarkly/go-server-sdk-dynamodb/v3"
)
```

2. When configuring your SDK client, add the DynamoDB data store as a `PersistentDataStore`. You may specify any custom DynamoDB options using the methods of `DynamoDBDataStoreBuilder`. For instance:

```go
    var config ld.Config{}
    config.DataStore = ldcomponents.PersistentDataStore(
        lddymamodb.DataStore("my-table-name").
            ClientOptions(dynamodb.Options{Region: "us-west-1"}),
    )
```

By default, the DynamoDB client will try to get your AWS credentials and region name from environment variables and/or local configuration files, as described in the AWS SDK documentation.

## Caching behavior

The LaunchDarkly SDK has a standard caching mechanism for any persistent data store, to reduce database traffic. This is configured through the SDK's `PersistentDataStoreBuilder` class as described the SDK documentation. For instance, to specify a cache TTL of 5 minutes:

```go
    var config ld.Config{}
    config.DataStore = ldcomponents.PersistentDataStore(
        lddynamodb.DataStore("my-table-name"),
    ).CacheMinutes(5)
```

## Data size limitation

DynamoDB has [a 400KB limit](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html#limits-items) on the size of any data item. For the LaunchDarkly SDK, a data item consists of the JSON representation of an individual feature flag or segment configuration, plus a few smaller attributes. You can see the format and size of these representations by querying `https://sdk.launchdarkly.com/flags/latest-all` and setting the `Authorization` header to your SDK key.

Most flags and segments won't be nearly as big as 400KB, but they could be if for instance you have a long list of user keys for individual user targeting. If the flag or segment representation is too large, it cannot be stored in DynamoDB. To avoid disrupting storage and evaluation of other unrelated feature flags, the SDK will simply skip storing that individual flag or segment, and will log a message (at ERROR level) describing the problem. For example:

```
    The item "my-flag-key" in "features" was too large to store in DynamoDB and was dropped
```

If caching is enabled in your configuration, the flag or segment may still be available in the SDK from the in-memory cache, but do not rely on this. If you see this message, consider redesigning your flag/segment configurations, or else do not use DynamoDB for the environment that contains this data item.

This limitation does not apply to target lists in [Big Segments](https://docs.launchdarkly.com/home/users/big-segments/).

A future version of the LaunchDarkly DynamoDB integration may use different strategies to work around this limitation, such as compressing the data or dividing it into multiple items. However, this integration is required to be interoperable with the DynamoDB integrations used by all the other LaunchDarkly SDKs and by the Relay Proxy, so any such change will only be made as part of a larger cross-platform release.

## LaunchDarkly overview

[LaunchDarkly](https://www.launchdarkly.com) is a feature management platform that serves trillions of feature flags daily to help teams build better software, faster. [Get started](https://docs.launchdarkly.com/docs/getting-started) using LaunchDarkly today!

## About LaunchDarkly

* LaunchDarkly is a continuous delivery platform that provides feature flags as a service and allows developers to iterate quickly and safely. We allow you to easily flag your features and manage them from the LaunchDarkly dashboard.  With LaunchDarkly, you can:
    * Roll out a new feature to a subset of your users (like a group of users who opt-in to a beta tester group), gathering feedback and bug reports from real-world use cases.
    * Gradually roll out a feature to an increasing percentage of users, and track the effect that the feature has on key metrics (for instance, how likely is a user to complete a purchase if they have feature A versus feature B?).
    * Turn off a feature that you realize is causing performance problems in production, without needing to re-deploy, or even restart the application with a changed configuration file.
    * Grant access to certain features based on user attributes, like payment plan (eg: users on the ‘gold’ plan get access to more features than users in the ‘silver’ plan). Disable parts of your application to facilitate maintenance, without taking everything offline.
* LaunchDarkly provides feature flag SDKs for a wide variety of languages and technologies. Read [our documentation](https://docs.launchdarkly.com/sdk) for a complete list.
* Explore LaunchDarkly
    * [launchdarkly.com](https://www.launchdarkly.com/ "LaunchDarkly Main Website") for more information
    * [docs.launchdarkly.com](https://docs.launchdarkly.com/  "LaunchDarkly Documentation") for our documentation and SDK reference guides
    * [apidocs.launchdarkly.com](https://apidocs.launchdarkly.com/  "LaunchDarkly API Documentation") for our API documentation
    * [blog.launchdarkly.com](https://blog.launchdarkly.com/  "LaunchDarkly Blog Documentation") for the latest product updates
