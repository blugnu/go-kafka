package confluent

import confluent "github.com/confluentinc/confluent-kafka-go/kafka"

func (c *adminClient) GetTopicMetadata(topic string, timeoutMs int) (*confluent.Metadata, error) {
	return c.funcs.GetMetadata(&topic, false, timeoutMs)
}
