# Change log

All notable changes to the LaunchDarkly Go SDK DynamoDB integration will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org).

## [1.1.0] - 2021-07-20
### Added:
- Added support for Big Segments. An Early Access Program for creating and syncing Big Segments from customer data platforms is available to enterprise customers.

## [1.0.1] - 2021-02-04
### Changed:
- Updated the default AWS SDK version to 1.37.2. Among other improvements as described in the [AWS Go SDK release notes](https://github.com/aws/aws-sdk-go/blob/master/CHANGELOG.md), this allows it to support [IAM roles for service accounts](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts-minimum-sdk.html).

## [1.0.0] - 2020-09-18
Initial release of the stand-alone version of this package to be used with versions 5.0.0 and above of the LaunchDarkly Server-Side SDK for Go.
