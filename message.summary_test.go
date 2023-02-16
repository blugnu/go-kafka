package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/blugnu/kafka/context"
	"github.com/blugnu/kafka/xfmt"
)

func equal[T comparable](a, b *T) bool {
	return (a == b) || (a != nil && b != nil && *a == *b)
}

func int32orNil(i *int32) string {
	if i == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", i)
}

func int64orNil(i *int64) string {
	if i == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", i)
}

func SorNil[T fmt.Stringer](p *T) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("%s", p)
}

func summariesAreEqual(wanted, got *context.MessageSummary) (bool, error) {
	if wanted.Topic != got.Topic {
		return false, fmt.Errorf("wanted Topic %q, got %q", wanted.Topic, got.Topic)
	}
	if wanted.TopicId != got.TopicId {
		return false, fmt.Errorf("wanted TopicId %q, got %q", wanted.TopicId, got.TopicId)
	}
	if !equal(wanted.Partition, got.Partition) {
		return false, fmt.Errorf("wanted Partition %s, got %s", int32orNil(wanted.Partition), int32orNil(got.Partition))
	}
	if !equal(wanted.Offset, got.Offset) {
		return false, fmt.Errorf("wanted Offset %s, got %s", int64orNil(wanted.Offset), int64orNil(got.Offset))
	}
	if wanted.Key != got.Key {
		return false, fmt.Errorf("wanted Key %q, got %q", wanted.Key, got.Key)
	}
	if !equal(wanted.TimeStamp, got.TimeStamp) {
		return false, fmt.Errorf("wanted TimeStamp %s, got %s", SorNil(wanted.TimeStamp), SorNil(got.TimeStamp))
	}
	if wanted.Age != got.Age {
		return false, fmt.Errorf("wanted Age %q, got %q", wanted.Age, got.Age)
	}
	if len(wanted.Headers) != len(got.Headers) {
		return false, fmt.Errorf("wanted Headers %q, got %q", wanted.Headers, got.Headers)
	}

	return true, nil
}

func TestMessage_Summary(t *testing.T) {
	// ARRANGE
	ptn := int32(1)
	off := int64(1)
	tms := time.Date(2010, 9, 8, 7, 6, 5, 432000000, time.UTC)
	age := time.Duration((4 * 24 * time.Hour) + (10 * time.Minute) + (5 * time.Second))

	testcases := []struct {
		name      string
		topic     string
		partition *int32
		offset    *int64
		key       []byte
		timestamp *time.Time
		age       *time.Duration
		result    *context.MessageSummary
	}{
		{name: "with age", topic: "topic", partition: &ptn, offset: &off, timestamp: &tms, age: &age, key: []byte("key"),
			result: &context.MessageSummary{
				Topic:     "topic",
				Partition: &ptn,
				Offset:    &off,
				TimeStamp: &tms,
				Age:       xfmt.Duration(age),
				Key:       "key",
				Headers:   map[string]string{"header": "header value"},
			},
		},
		{name: "without age", topic: "topic", partition: &ptn, offset: &off, timestamp: &tms, key: []byte("key"),
			result: &context.MessageSummary{
				Topic:     "topic",
				Partition: &ptn,
				Offset:    &off,
				TimeStamp: &tms,
				Key:       "key",
				Headers:   map[string]string{"header": "header value"},
			},
		},
		{name: "with binary key", topic: "topic", partition: &ptn, offset: &off, timestamp: &tms, key: []byte{0x00, 0x01, 0x02},
			result: &context.MessageSummary{
				Topic:     "topic",
				Partition: &ptn,
				Offset:    &off,
				TimeStamp: &tms,
				Key:       string([]byte{0x00, 0x01, 0x02}),
				Headers:   map[string]string{"header": "header value"},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			msg := &Message{
				Topic:     tc.topic,
				Partition: tc.partition,
				Offset:    tc.offset,
				Timestamp: tc.timestamp,
				Age:       tc.age,
				Key:       tc.key,
				Headers:   Headers{"header": "header value"},
			}

			// ACT
			s := msg.summary()

			// ASSERT
			wanted := tc.result
			got := s
			if _, err := summariesAreEqual(wanted, got); err != nil {
				t.Error(err)
			}
		})
	}
}
