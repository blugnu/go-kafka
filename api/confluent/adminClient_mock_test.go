package confluent

import confluent "github.com/confluentinc/confluent-kafka-go/kafka"

type MockAdminClient struct {
	funcs *adminClientFuncs
}

func (c *MockAdminClient) Close() {
	c.funcs.Close()
}

func (c *MockAdminClient) GetTopicMetadata(topic string, timeoutMs int) (*confluent.Metadata, error) {
	return c.funcs.GetMetadata(&topic, false, timeoutMs)
}
