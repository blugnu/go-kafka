package api

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type consumer struct {
	*kafka.Consumer
}

var _consumer = &consumer{}

func ConfluentConsumerApi() ConsumerApi {
	return _consumer
}

func (c *consumer) Close() error {
	return c.Consumer.Close()
}

func (c *consumer) Commit(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return c.CommitMessage(msg)
}

func (c *consumer) CommitOffset(tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return c.CommitOffsets(tpa)
}

func (c *consumer) Create(cfg *kafka.ConfigMap) error {
	if cfg == nil {
		c.Consumer = &kafka.Consumer{}
		return nil
	}

	var err error
	c.Consumer, err = kafka.NewConsumer(cfg)
	return err
}

func (c *consumer) Subscribe(ta []string, rcb kafka.RebalanceCb) error {
	return c.SubscribeTopics(ta, rcb)
}

func (c *consumer) ReadMessage(t time.Duration) (*kafka.Message, error) {
	return c.Consumer.ReadMessage(t)
}
