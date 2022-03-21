# Contributing to this library

The source code for this library is [here](https://github.com/launchdarkly/go-server-sdk-dynamodb). We encourage pull-requests and other contributions from the community. Since this library is meant to be used in conjunction with the LaunchDarkly Go SDK, you may want to look at the [Go SDK source code](https://github.com/launchdarkly/go-server-sdk) and our [SDK contributor's guide](https://docs.launchdarkly.com/sdk/concepts/contributors-guide).

## Submitting bug reports and feature requests
 
The LaunchDarkly SDK team monitors the [issue tracker](https://github.com/launchdarkly/go-server-sdk-dynamodb/issues) in this repository. Bug reports and feature requests specific to this project should be filed in the issue tracker. The SDK team will respond to all newly filed issues within two business days.
 
## Submitting pull requests
 
We encourage pull requests and other contributions from the community. Before submitting pull requests, ensure that all temporary or unintended code is removed. Don't worry about adding reviewers to the pull request; the LaunchDarkly SDK team will add themselves. The SDK team will acknowledge all pull requests within two business days.
 
## Build instructions
 
### Prerequisites

This project should be built against the lowest supported Go version as described in [README.md](./README.md).

### Building

To build the library without running any tests:
```
make
```

### Testing
 
To build the library and run all unit tests:
```
make test
```

The tests expect you to have DynamoDB running locally. One way to do this is with Docker:

```
docker run -p 8000:8000 amazon/dynamodb-local
```
