package confluent

import (
	"github.com/blugnu/kafka/api"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

var newConsumer = confluent.NewConsumer

func (c *Consumer) Create(cfg *api.ConfigMap) (err error) {
	cm := &confluent.ConfigMap{}
	for k, v := range *cfg {
		cm.SetKey(k, v)
	}

	// Request that librd client logs are sent down a go channel
	// rather than 'polluting' stderr
	cm.SetKey("go.logs.channel.enable", true)

	if c.Consumer, err = newConsumer(cm); err != nil {
		return err
	}

	// Hookup funcs to the consumer receivers
	c.funcs = &ConsumerFuncs{
		Assign:          c.Consumer.Assign,
		Close:           c.Consumer.Close,
		CommitOffsets:   c.Consumer.CommitOffsets,
		Logs:            c.Consumer.Logs,
		Position:        c.Consumer.Position,
		ReadMessage:     c.Consumer.ReadMessage,
		Seek:            c.Consumer.Seek,
		SubscribeTopics: c.Consumer.SubscribeTopics,
		Unassign:        c.Consumer.Unassign,
	}

	return initConsumer(c)
}
