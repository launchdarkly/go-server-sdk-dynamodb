package lddynamodb

import (
	"os"
	"testing"

	"gopkg.in/launchdarkly/go-sdk-common.v2/ldvalue"
	"gopkg.in/launchdarkly/go-server-sdk.v5/interfaces"
	"gopkg.in/launchdarkly/go-server-sdk.v5/ldcomponents"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataSourceBuilder(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		b := DataStore("t")
		assert.Nil(t, b.client)
		assert.Len(t, b.configs, 0)
		assert.Equal(t, "", b.prefix)
		assert.Equal(t, session.Options{}, b.sessionOptions)
		assert.Equal(t, "t", b.table)
	})

	t.Run("ClientConfig", func(t *testing.T) {
		c1 := &aws.Config{MaxRetries: aws.Int(1)}
		c2 := &aws.Config{MaxRetries: aws.Int(2)}

		b := DataStore("t").ClientConfig(c1).ClientConfig(c2)
		assert.Equal(t, []*aws.Config{c1, c2}, b.configs)
	})

	t.Run("DynamoClient", func(t *testing.T) {
		sess, err := session.NewSessionWithOptions(session.Options{})
		require.NoError(t, err)
		client := dynamodb.New(sess)

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

	t.Run("SessionOptions", func(t *testing.T) {
		s := session.Options{Profile: "x"}

		b := DataStore("t").SessionOptions(s)
		assert.Equal(t, s, b.sessionOptions)
	})

	t.Run("error for empty table name", func(t *testing.T) {
		ds, err := DataStore("").CreatePersistentDataStore(simpleTestContext{})
		assert.Error(t, err)
		assert.Nil(t, ds)

		bs, err := DataStore("").CreateBigSegmentStore(simpleTestContext{})
		assert.Error(t, err)
		assert.Nil(t, bs)
	})

	t.Run("error for invalid configuration", func(t *testing.T) {
		os.Setenv("AWS_CA_BUNDLE", "not a real CA file")
		defer os.Setenv("AWS_CA_BUNDLE", "")

		ds, err := DataStore("t").CreatePersistentDataStore(simpleTestContext{})
		assert.Error(t, err)
		assert.Nil(t, ds)

		bs, err := DataStore("t").CreateBigSegmentStore(simpleTestContext{})
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
