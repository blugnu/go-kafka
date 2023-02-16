package confluent

import (
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/blugnu/kafka/api"
)

func (c *Consumer) ReadMessage(t time.Duration) (*api.Message, error) {
	kmsg, err := c.funcs.ReadMessage(t)
	if err, isConfluentError := err.(confluent.Error); kmsg == nil && isConfluentError && err.Code() == confluent.ErrTimedOut {
		return nil, api.ErrTimeout
	}
	if err != nil {
		return nil, err
	}

	msg := &api.Message{
		Topic:     *kmsg.TopicPartition.Topic,
		Partition: &kmsg.TopicPartition.Partition,
		Offset:    (*int64)(&kmsg.TopicPartition.Offset),
		Key:       kmsg.Key,
		Value:     kmsg.Value,
		Timestamp: &kmsg.Timestamp,
	}

	return msg, nil
}
