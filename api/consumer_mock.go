package api

import (
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConsumerApiMock interface {
	ConsumerApi
	Api() *consumerApi
	Messages([]interface{})
}

type consumerApi struct {
	Create       func(cfg *kafka.ConfigMap) error
	Close        func() error
	Commit       func(msg *kafka.Message) ([]kafka.TopicPartition, error)
	CommitOffset func(tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	Subscribe    func(ta []string, rcb kafka.RebalanceCb) error
}

type mockConsumer struct {
	api          consumerApi
	messages     []interface{}
	messageIndex int
}

func MockConsumerApi() (ConsumerApi, ConsumerApiMock) {
	result := &mockConsumer{
		messages: []interface{}{},
		api: consumerApi{
			Create: func(cfg *kafka.ConfigMap) error { return nil },
			Close:  func() error { return nil },
			Commit: func(msg *kafka.Message) ([]kafka.TopicPartition, error) {
				return []kafka.TopicPartition{msg.TopicPartition}, nil
			},
			CommitOffset: func(tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error) { return tpa, nil },
			Subscribe:    func(ta []string, rcb kafka.RebalanceCb) error { return nil },
		},
	}
	return result, result
}

func (c *mockConsumer) Api() *consumerApi {
	return &c.api
}

func (c *mockConsumer) Messages(msgs []interface{}) {
	c.messages = append(c.messages, msgs...)
}

func (c *mockConsumer) Create(cfg *kafka.ConfigMap) error {
	return c.api.Create(cfg)
}

func (c *mockConsumer) Close() error {
	return c.api.Close()
}

func (c *mockConsumer) Commit(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return c.api.Commit(msg)
}

func (c *mockConsumer) CommitOffset(tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return c.api.CommitOffset(tpa)
}

func (c *mockConsumer) Subscribe(topics []string, rebalanceCallback kafka.RebalanceCb) error {
	return c.api.Subscribe(topics, rebalanceCallback)
}

func (c *mockConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	if c.messageIndex >= len(c.messages) {
		return nil, errors.New("no more messages")
	}

	msg := c.messages[c.messageIndex]
	c.messageIndex++

	switch msg := msg.(type) {
	case *kafka.Message:
		return msg, nil
	case error:
		return nil, msg
	}
	return nil, fmt.Errorf("unexpected item of type %T in mock message list", msg)
}
