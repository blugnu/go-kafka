package kafka

import (
	"github.com/blugnu/kafka/context"
	"github.com/blugnu/kafka/xfmt"
)

func (msg *Message) summary() *context.MessageSummary {
	topic := msg.Topic
	topicId, mapped := IdForTopic(topic)
	if !mapped {
		topicId = "" // If there is no topic:topicid mapping then we only care about "topic"
	}

	var age string
	if msg.Age != nil {
		age = xfmt.Duration(*msg.Age)
	}

	return &context.MessageSummary{
		Topic:     topic,
		TopicId:   topicId,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       string(msg.Key),
		Headers:   msg.Headers,
		TimeStamp: msg.Timestamp,
		Age:       age,
	}
}
