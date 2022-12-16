package kafka

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type topicHandlerMap map[string]TopicHandler

func (thm topicHandlerMap) copy() topicHandlerMap {
	copy := topicHandlerMap{}
	for k, v := range thm {
		copy[k] = TopicHandler{
			Func:        v.Func,
			RetryPolicy: v.RetryPolicy,
		}
	}
	return copy
}

type TopicHandler struct {
	DeadLetterTopic string
	RetryPolicy     RetryPolicy
	RetryTopic      *string
	Func            func(context.Context, *kafka.Message) error
}

type RetryPolicy interface {
	CanRetry(*kafka.Message) bool
	Wait(*kafka.Message)
}

type LinearRetry struct {
	Wait       time.Duration
	MaxRetries int
	TopicId    string
}

type ExponentialBackoff struct {
	InitialWait time.Duration
	MaxWait     time.Duration
	MaxRetries  int
	TopicId     string
}
