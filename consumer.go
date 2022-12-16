package kafka

import (
	"context"
	"fmt"

	confluent "github.com/blugnu/kafka/api"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	api        confluent.ConsumerApi
	config     *config
	handlers   topicHandlerMap
	middleware MessageHandler
	isRunning  bool
}

func sliceToMap[T any, K comparable, V any](s []T, fn func(T) (K, V)) map[K]V {
	result := map[K]V{}
	for _, i := range s {
		k, v := fn(i)
		result[k] = v
	}
	return result
}

func NewConsumer(cfg *config) (*Consumer, error) {
	// Assume standard consumer api by default
	api := confluent.ConfluentConsumerApi()

	// Apply any alternative (i.e. mocked) api from the config (if valid)
	if cfg.api != nil {
		var ok bool
		if api, ok = cfg.api.(confluent.ConsumerApi); !ok {
			panic(fmt.Sprintf("api (%T) not valid for a consumer", cfg.api))
		}
	}

	cmap := cfg.config.configMap()
	if cfg.HasNoBroker() {
		cmap = nil
	}

	// Create the consumer
	var err error
	if err = api.Create(cmap); err != nil {
		return nil, err
	}

	return &Consumer{
		api:        api,
		config:     cfg.copy(),
		middleware: cfg.middleware,
		handlers:   cfg.topicHandlers.copy(),
	}, nil
}

func (c *Consumer) Close() {
	c.isRunning = false
	c.api.Close()
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.Close()

	autoCommit := c.config.autoCommit()
	readCommitted := c.config.readCommitted

	rcb := c.config.rcb

	if err := c.api.Subscribe(c.config.messageHandlers.topicIds(), rcb); err != nil {
		return err
	}

	c.isRunning = true
	for {
		msg, err := c.api.ReadMessage(-1)
		if err != nil {
			return ReadError{err, msg}
		}
		if msg == nil {
			continue
		}

		if readCommitted {
			_, err = c.api.Commit(msg)
			if err != nil {
				return CommitError{err, msg}
			}
		}

		if c.middleware != nil {
			err = c.middleware(ctx, msg)
			if err != nil {
				return MiddlewareError{err, msg}
			}
		}

		handler := c.handlers[*msg.TopicPartition.Topic]
		if handler.RetryPolicy != nil {
			handler.RetryPolicy.Wait(msg)
		}

		if err = handler.Func(ctx, msg); err != nil {
			if handler.RetryPolicy == nil || handler.RetryPolicy.CanRetry(msg) {
				// TODO: DeadLetterDrop if possible, otherwise return with error
				return HandlerError{err, msg}
			}
			retryTopic := *msg.TopicPartition.Topic
			if handler.RetryTopic != nil {
				retryTopic = *handler.RetryTopic
			}

			// TODO: Post Retry Event
			retry := &kafka.Message{}
			headers := sliceToMap(msg.Headers, func(h kafka.Header) (string, []byte) { return h.Key, h.Value })
			id := headers["x-retry-id"]
			attempt := headers["x-retry-attempt"] + 1
			retry.Headers = append(retry.Headers, kafka.Header{Key: "x-retry-id", Value: id})

			Produce(retryTopic, retry)
		}

		if !autoCommit {
			_, err = c.api.Commit(msg)
			if err != nil {
				return CommitError{err, msg}
			}
		}
	}
}
