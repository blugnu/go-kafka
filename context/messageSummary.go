package context

import "time"

type MessageSummary struct {
	Topic     string            `json:"topic"`
	TopicId   string            `json:"topicId,omitempty"`
	Partition *int32            `json:"partition,omitempty"`
	Offset    *int64            `json:"offset,omitempty"`
	Key       string            `json:"key,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	TimeStamp *time.Time        `json:"timestamp,omitempty"`
	Age       string            `json:"age,omitempty"`
}
