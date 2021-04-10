module github.com/launchdarkly/go-server-sdk-dynamodb

go 1.13

require (
	github.com/aws/aws-sdk-go v1.37.2
	github.com/stretchr/testify v1.6.1
	gopkg.in/launchdarkly/go-sdk-common.v2 v2.3.0
	gopkg.in/launchdarkly/go-server-sdk.v5 v5.0.0
)

replace gopkg.in/launchdarkly/go-sdk-common.v2 => github.com/launchdarkly/go-sdk-common-private/v2 v2.2.3-0.20210323175925-2f53ef23e94c

replace gopkg.in/launchdarkly/go-server-sdk-evaluation.v1 => github.com/launchdarkly/go-server-sdk-evaluation-private v1.2.1-0.20210323201644-112b8c0df0c7

replace gopkg.in/launchdarkly/go-server-sdk.v5 => github.com/launchdarkly/go-server-sdk-private/v5 v5.2.2-0.20210331020335-1b360baf49c6
