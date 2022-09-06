package lddynamodb

import (
	"testing"

	"gopkg.in/launchdarkly/go-sdk-common.v2/ldvalue"
	"gopkg.in/launchdarkly/go-server-sdk.v5/interfaces"
	"gopkg.in/launchdarkly/go-server-sdk.v5/ldcomponents"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestDataSourceBuilder(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		b := DataStore("t")
		assert.Nil(t, b.client)
		assert.Nil(t, b.awsConfig)
		assert.Equal(t, "", b.prefix)
		assert.Equal(t, dynamodb.Options{}, b.clientOptions)
		assert.Len(t, b.clientOpFns, 0)
		assert.Equal(t, "t", b.table)
	})

	t.Run("ClientConfig", func(t *testing.T) {
		conf := aws.Config{RetryMaxAttempts: 1}
		optFn1 := func(*dynamodb.Options) {}
		optFn2 := func(*dynamodb.Options) {}

		b := DataStore("t").ClientConfig(conf, optFn1, optFn2)
		assert.Equal(t, &conf, b.awsConfig)
		assert.Len(t, b.clientOpFns, 2)
	})

	t.Run("ClientOptions", func(t *testing.T) {
		opt := dynamodb.Options{ClientLogMode: aws.LogRequestEventMessage}
		optFn1 := func(*dynamodb.Options) {}
		optFn2 := func(*dynamodb.Options) {}

		b := DataStore("t").ClientOptions(opt, optFn1, optFn2)
		assert.Nil(t, b.awsConfig)
		assert.Equal(t, opt, b.clientOptions)
		assert.Len(t, b.clientOpFns, 2)
	})

	t.Run("DynamoClient", func(t *testing.T) {
		client := dynamodb.New(dynamodb.Options{})

		b := DataStore("t").DynamoClient(client)
		assert.Equal(t, client, b.client)
	})

	t.Run("Prefix", func(t *testing.T) {
		b := DataStore("t").Prefix("p")
		assert.Equal(t, "p", b.prefix)

		// Unlike our other database integrations, in DynamoDB we allow the prefix to be empty. This is
		// because it's unlikely for multiple applications to be sharing the same DynamoDB table; that
		// would be impractical because of the need to configure throttling on a per-table basis.
		b.Prefix("")
		assert.Equal(t, "", b.prefix)
	})

	t.Run("error for empty table name", func(t *testing.T) {
		ds, err := DataStore("").CreatePersistentDataStore(simpleTestContext{})
		assert.Error(t, err)
		assert.Nil(t, ds)

		bs, err := DataStore("").CreateBigSegmentStore(simpleTestContext{})
		assert.Error(t, err)
		assert.Nil(t, bs)
	})

	t.Run("diagnostic description", func(t *testing.T) {
		value := DataStore("").DescribeConfiguration()
		assert.Equal(t, ldvalue.String("DynamoDB"), value)
	})
}

// stub implementation of interfaces.ClientContext
type simpleTestContext struct{}

func (c simpleTestContext) GetBasic() interfaces.BasicConfiguration {
	return interfaces.BasicConfiguration{}
}

func (c simpleTestContext) GetHTTP() interfaces.HTTPConfiguration {
	return nil
}

func (c simpleTestContext) GetLogging() interfaces.LoggingConfiguration {
	lc, _ := ldcomponents.Logging().CreateLoggingConfiguration(interfaces.BasicConfiguration{})
	return lc
}
