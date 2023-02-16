package confluent

import (
	"time"

	"github.com/blugnu/logger"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/blugnu/kafka/api"
)

type ConsumerFuncs struct {
	Assign          func([]confluent.TopicPartition) error
	Close           func() error
	CommitOffsets   func([]confluent.TopicPartition) ([]confluent.TopicPartition, error)
	Logs            func() chan confluent.LogEvent
	Position        func([]confluent.TopicPartition) ([]confluent.TopicPartition, error)
	ReadMessage     func(time.Duration) (*confluent.Message, error)
	Seek            func(confluent.TopicPartition, int) error
	SubscribeTopics func([]string, confluent.RebalanceCb) error
	Unassign        func() error
}

type Consumer struct {
	*confluent.Consumer
	funcs       *ConsumerFuncs
	Log         *logger.Base
	OnRebalance api.RebalanceEventHandler
	AdminClient
}
