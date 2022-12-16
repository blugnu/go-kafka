package api

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConsumerApi interface {
	Create(*kafka.ConfigMap) error
	Close() error
	Commit(*kafka.Message) ([]kafka.TopicPartition, error)
	CommitOffset([]kafka.TopicPartition) ([]kafka.TopicPartition, error)
	ReadMessage(time.Duration) (*kafka.Message, error)
	Subscribe(ta []string, rcb kafka.RebalanceCb) error
}
