# LaunchDarkly Server-side SDK for Go - Dynamodb integration

[![Circle CI](https://circleci.com/gh/launchdarkly/go-server-sdk-dynamodb.svg?style=shield)](https://circleci.com/gh/launchdarkly/go-server-sdk-dynamodb) [![Documentation](https://img.shields.io/static/v1?label=go.dev&message=reference&color=00add8)](https://pkg.go.dev/github.com/launchdarkly/go-server-sdk-dynamodb)

This library provides a [DynamoDB](https://aws.amazon.com/dynamodb/)-backed persistence mechanism (data store) for the [LaunchDarkly Go SDK](https://github.com/launchdarkly/go-server-sdk), replacing the default in-memory data store.

This version of the library requires at least version 5.0.0 of the LaunchDarkly Go SDK. In earlier Go SDK versions, the `lddynamodb` package was built into the SDK (`gopkg.in/launchdarkly/go-server-sdk.v4/lddynamodb`).

The minimum Go version is 1.13.

For more information, see also: [Using a persistent feature store](https://docs.launchdarkly.com/v2.0/docs/using-a-persistent-feature-store).

## This is prerelease code

This project is currently in beta. The full 1.0.0 release will be done when Go SDK 5.0.0 is released.

## Quick setup

This assumes that you have already installed the LaunchDarkly Go SDK.

1. Import the LaunchDarkly SDK packages and the package for this library:

```go
import (
    ld "gopkg.in/launchdarkly/go-server-sdk.v5"
    "gopkg.in/launchdarkly/go-server-sdk.v5/ldcomponents"
    lddynamodb "github.com/launchdarkly/go-server-sdk-dynamodb"
)
```

2. When configuring your SDK client, add the DynamoDB data store as a `PersistentDataStore`. You may specify any custom DynamoDB options using the methods of `DynamoDBDataStoreBuilder`. For instance:

```go
    var config ld.Config{}
    config.DataStore = ldcomponents.PersistentDataStore(
        lddymamodb.DataStore("my-table-name").
            SessionOptions(session.Options{Profile: "profile_name"}),
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

## LaunchDarkly overview

[LaunchDarkly](https://www.launchdarkly.com) is a feature management platform that serves over 100 billion feature flags daily to help teams build better software, faster. [Get started](https://docs.launchdarkly.com/docs/getting-started) using LaunchDarkly today!

## About LaunchDarkly

* LaunchDarkly is a continuous delivery platform that provides feature flags as a service and allows developers to iterate quickly and safely. We allow you to easily flag your features and manage them from the LaunchDarkly dashboard.  With LaunchDarkly, you can:
    * Roll out a new feature to a subset of your users (like a group of users who opt-in to a beta tester group), gathering feedback and bug reports from real-world use cases.
    * Gradually roll out a feature to an increasing percentage of users, and track the effect that the feature has on key metrics (for instance, how likely is a user to complete a purchase if they have feature A versus feature B?).
    * Turn off a feature that you realize is causing performance problems in production, without needing to re-deploy, or even restart the application with a changed configuration file.
    * Grant access to certain features based on user attributes, like payment plan (eg: users on the ‘gold’ plan get access to more features than users in the ‘silver’ plan). Disable parts of your application to facilitate maintenance, without taking everything offline.
* LaunchDarkly provides feature flag SDKs for a wide variety of languages and technologies. Check out [our documentation](https://docs.launchdarkly.com/docs) for a complete list.
* Explore LaunchDarkly
    * [launchdarkly.com](https://www.launchdarkly.com/ "LaunchDarkly Main Website") for more information
    * [docs.launchdarkly.com](https://docs.launchdarkly.com/  "LaunchDarkly Documentation") for our documentation and SDK reference guides
    * [apidocs.launchdarkly.com](https://apidocs.launchdarkly.com/  "LaunchDarkly API Documentation") for our API documentation
    * [blog.launchdarkly.com](https://blog.launchdarkly.com/  "LaunchDarkly Blog Documentation") for the latest product updates
    * [Feature Flagging Guide](https://github.com/launchdarkly/featureflags/  "Feature Flagging Guide") for best practices and strategies
