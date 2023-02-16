package confluent

import (
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/blugnu/kafka/api"
)

func (c *Consumer) Seek(msg *api.Offset, timeout time.Duration) error {
	c.Log.Log().Debugf("seeking to offset %s", msg)

	tpa := confluent.TopicPartition{
		Topic:     &msg.Topic,
		Partition: msg.Partition,
		Offset:    confluent.Offset(msg.Offset),
	}
	return c.funcs.Seek(tpa, int(timeout.Milliseconds()))
}
