package kafka

import (
	"bytes"
	"testing"
	"time"

	"github.com/blugnu/kafka/api"
)

func ApiMessagesEqual(a, b *api.Message) bool {
	if a == nil {
		return b == nil
	}

	if sameOffset := a.Topic == b.Topic &&
		((a.Partition == nil && b.Partition == nil) || ((a.Partition != nil && b.Partition != nil) && (*a.Partition == *b.Partition))) &&
		((a.Offset == nil && b.Offset == nil) || ((a.Offset != nil && b.Offset != nil) && (*a.Offset == *b.Offset))); !sameOffset {
		return false
	}

	if sameKey := bytes.Equal(a.Key, b.Key); !sameKey {
		return false
	}

	if sameValue := bytes.Equal(a.Value, b.Value); !sameValue {
		return false
	}

	if sameTimestamp := (a.Timestamp == nil) && (b.Timestamp == nil) ||
		((a.Timestamp != nil && b.Timestamp != nil) && (*a.Timestamp == *b.Timestamp)); !sameTimestamp {
		return false
	}

	if len(a.Headers) != len(b.Headers) {
		return false
	}

	keys := []string{}
	for k := range a.Headers {
		keys = append(keys, k)
	}
	for _, k := range keys {
		if !bytes.Equal(a.Headers[k], b.Headers[k]) {
			return false
		}
	}

	return true
}

func TestMessage_Stringer(t *testing.T) {
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
		timestamp *time.Time
		age       *time.Duration
		result    string
	}{
		{name: "with offset, timestamp and age", topic: "topic", partition: &ptn, offset: &off, timestamp: &tms, age: &age, result: "topic @ 1:1 [2010-09-08T07:06:05.432Z / 4d 0h 10m 5.000s]"},
		{name: "with offset and timestamp", topic: "topic", partition: &ptn, offset: &off, timestamp: &tms, result: "topic @ 1:1 [2010-09-08T07:06:05.432Z]"},
		{name: "with offset only", topic: "topic", partition: &ptn, offset: &off, result: "topic @ 1:1"},
		{name: "with topic only", topic: "topic", result: "topic @ <not set>:<not set>"},
		{name: "with nothing", result: "<no topic> @ <not set>:<not set>"},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			sut := &Message{
				Topic:     tc.topic,
				Partition: tc.partition,
				Offset:    tc.offset,
				Timestamp: tc.timestamp,
				Age:       tc.age,
			}

			// ACT
			s := sut.String()

			// ASSERT
			wanted := tc.result
			got := s
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}
}

func TestMessage_FromApiMessage(t *testing.T) {
	ptn := int32(1)
	off := int64(1)
	tms := time.Date(2010, 9, 8, 7, 6, 5, 432000000, time.UTC)

	testcases := []struct {
		name   string
		src    *api.Message
		result *Message
	}{
		{
			name:   "with timestamp",
			src:    &api.Message{Topic: "topic", Partition: &ptn, Offset: &off, Key: []byte("key"), Value: []byte("value"), Timestamp: &tms, Headers: api.Headers{"header": []byte("header value")}},
			result: &Message{Topic: "topic", Partition: &ptn, Offset: &off, Key: []byte("key"), Value: []byte("value"), Timestamp: &tms, Headers: Headers{"header": "header value"}, RawHeaders: RawHeaders{"header": []byte("header value")}},
		},
		{
			name:   "no timestamp",
			src:    &api.Message{Topic: "topic", Partition: &ptn, Offset: &off, Key: []byte("key"), Value: []byte("value"), Headers: api.Headers{"header": []byte("header value")}},
			result: &Message{Topic: "topic", Partition: &ptn, Offset: &off, Key: []byte("key"), Value: []byte("value"), Headers: Headers{"header": "header value"}, RawHeaders: RawHeaders{"header": []byte("header value")}},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			msg := fromApiMessage(tc.src)

			// ASSERT
			wanted := tc.result
			got := msg
			if !wanted.Equal(got) {
				t.Errorf("wanted %#v, got %#v", wanted, got)
			}
		})
	}
}

func TestMessage_AsApiMessage(t *testing.T) {
	ptn := int32(1)
	off := int64(1)
	tms := time.Date(2010, 9, 8, 7, 6, 5, 432000000, time.UTC)

	testcases := []struct {
		name   string
		src    *Message
		result *api.Message
	}{
		{
			name:   "with timestamp",
			src:    &Message{Topic: "topic", Partition: &ptn, Offset: &off, Key: []byte("key"), Value: []byte("value"), Timestamp: &tms, Headers: Headers{"header": "header value"}, RawHeaders: RawHeaders{"header": []byte("header value")}},
			result: &api.Message{Topic: "topic", Partition: &ptn, Offset: &off, Key: []byte("key"), Value: []byte("value"), Timestamp: &tms, Headers: api.Headers{"header": []byte("header value")}},
		},
		{
			name:   "no timestamp",
			src:    &Message{Topic: "topic", Partition: &ptn, Offset: &off, Key: []byte("key"), Value: []byte("value"), Headers: Headers{"header": "header value"}, RawHeaders: RawHeaders{"header": []byte("header value")}},
			result: &api.Message{Topic: "topic", Partition: &ptn, Offset: &off, Key: []byte("key"), Value: []byte("value"), Headers: api.Headers{"header": []byte("header value")}},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			msg := tc.src.asApiMessage()

			// ASSERT
			wanted := tc.result
			got := msg
			if !ApiMessagesEqual(wanted, got) {
				t.Errorf("wanted %#v, got %#v", wanted, got)
			}
		})
	}
}
