package kafka

import (
	"context"
)

// subscribe subscribes the consumer to the topics required as determined
// by the message handlers configured on the consumer.
//
// The consumer will be subscribed to any topics for which a message handler
// is configured as well as to any RetryTopics configured by those handlers.
func (c *Consumer) subscribe(ctx context.Context) error {
	log := c.logEntry()

	topics := c.MessageHandlers.topicIds()
	if err := c.api.Subscribe(topics, c.OnRebalance); err != nil {
		return ApiError{"Subscribe", err}
	}

	log.WithField("kafka.topics", topics).
		Debug("consumer subscribed")

	return nil
}
