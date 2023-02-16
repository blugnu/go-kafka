package confluent

import (
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/blugnu/kafka/api"
)

var intentAdjustment = map[api.CommitIntent]int{
	api.ReadAgain: 0,
	api.ReadNext:  +1,
}

func (c *Consumer) Commit(offset *api.Offset, intent api.CommitIntent) error {
	c.Log.Log().Debugf("committing offset %s with intent %s", offset, intent)

	tpa := []confluent.TopicPartition{
		{
			Topic:     &offset.Topic,
			Partition: offset.Partition,
			Offset:    confluent.Offset(offset.Offset + int64(intentAdjustment[intent])),
		},
	}
	_, err := c.funcs.CommitOffsets(tpa)
	return err
}
