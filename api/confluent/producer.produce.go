package confluent

import (
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/blugnu/kafka/api"
)

func (p *Producer) Produce(msg *api.Message, ch chan confluent.Event) error {
	ptn := confluent.PartitionAny
	if msg.Partition != nil {
		ptn = *msg.Partition
	}

	kmsg := &confluent.Message{
		TopicPartition: confluent.TopicPartition{
			Topic:     &msg.Topic,
			Partition: ptn,
			Offset:    confluent.OffsetInvalid,
		},
		Key:   msg.Key,
		Value: msg.Value,
	}

	for k, v := range msg.Headers {
		kmsg.Headers = append(kmsg.Headers, confluent.Header{Key: k, Value: v})
	}

	return p.funcs.Produce(kmsg, ch)
}
