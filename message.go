package kafka

import (
	"fmt"
	"strconv"
	"time"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/xfmt"
)

type Message struct {
	Topic     string
	Partition *int32
	Offset    *int64
	Key       []byte
	Value     []byte
	Timestamp *time.Time     // The Timestamp of the message (non-nil only for messages read by a consumer)
	Age       *time.Duration // The Age of the message at the time that it was read by the consumer.  To get the current age of a message use time.Since(*msg.Timestamp)
	Headers
	RawHeaders
}

func (msg *Message) String() string {
	topic := "<no topic>"
	partition := "<not set>"
	offset := "<not set>"

	if msg.Topic != "" {
		topic = msg.Topic
	}

	if msg.Partition != nil {
		partition = strconv.Itoa(int(*msg.Partition))
	}

	if msg.Offset != nil {
		offset = strconv.Itoa(int(*msg.Offset))
	}

	ts := ""
	if msg.Timestamp != nil {
		ts = (*msg.Timestamp).Format("2006-01-02T15:04:05.000Z")
		if msg.Age != nil {
			ts = ts + " / " + xfmt.Duration(*msg.Age)
		}
		ts = fmt.Sprintf(" [%s]", ts)
	}

	return fmt.Sprintf("%s @ %s:%s%s", topic, partition, offset, ts)
}

func fromApiMessage(apimsg *api.Message) *Message {
	topic, _ := TopicForId(apimsg.Topic)

	msg := &Message{
		Headers:    Headers{},
		RawHeaders: RawHeaders{},
		Key:        apimsg.Key,
		Topic:      topic,
		Partition:  apimsg.Partition,
		Offset:     apimsg.Offset,
		Value:      apimsg.Value,
	}

	timestamp := apimsg.Timestamp
	if timestamp != nil && !(*timestamp).IsZero() {
		age := time.Since(*timestamp)
		msg.Timestamp = timestamp
		msg.Age = &age
	}

	for k, v := range apimsg.Headers {
		msg.Headers[k] = string(v)
		msg.RawHeaders[k] = v
	}

	return msg
}

func (msg *Message) asApiMessage() *api.Message {
	topic, _ := IdForTopic(msg.Topic)

	apimsg := &api.Message{
		Key:       msg.Key,
		Topic:     topic,
		Value:     msg.Value,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
		Headers:   api.Headers{},
	}

	for k, v := range msg.RawHeaders {
		apimsg.Headers[k] = v
	}

	return apimsg
}
