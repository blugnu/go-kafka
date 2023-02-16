package api

import (
	"testing"
)

func TestMessage_GetOffset(t *testing.T) {
	ptn := int32(1)
	off := int64(42)

	testcases := []struct {
		name     string
		message  Message
		expected Offset
	}{
		{name: "no partition or offset", message: Message{Topic: "topic"}, expected: Offset{"topic", -1, -1}},
		{name: "no partition", message: Message{Topic: "topic", Offset: &off}, expected: Offset{"topic", -1, 42}},
		{name: "no offset", message: Message{Topic: "topic", Partition: &ptn}, expected: Offset{"topic", 1, -1}},
		{name: "complete", message: Message{Topic: "topic", Partition: &ptn, Offset: &off}, expected: Offset{"topic", 1, 42}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			msg := &tc.message
			got := msg.GetOffset()

			// ASSERT
			wanted := &tc.expected
			if *wanted != *got {
				t.Errorf("wanted %#v, got %#v", wanted, got)
			}
		})
	}
}

func TestMessage_String(t *testing.T) {
	ptn := int32(1)
	off := int64(42)

	testcases := []struct {
		name     string
		message  Message
		expected string
	}{
		{name: "no value, partition or offset", message: Message{Topic: "topic"}, expected: "0 bytes @ topic,-1:-1"},
		{name: "no partition or offset", message: Message{Topic: "topic", Value: []byte("value")}, expected: "5 bytes @ topic,-1:-1"},
		{name: "no partition", message: Message{Topic: "topic", Offset: &off, Value: []byte("value")}, expected: "5 bytes @ topic,-1:42"},
		{name: "no offset", message: Message{Topic: "topic", Partition: &ptn, Value: []byte("value")}, expected: "5 bytes @ topic,1:-1"},
		{name: "complete", message: Message{Topic: "topic", Partition: &ptn, Offset: &off, Value: []byte("value")}, expected: "5 bytes @ topic,1:42"},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			msg := &tc.message
			got := msg.String()

			// ASSERT
			wanted := tc.expected
			if wanted != got {
				t.Errorf("wanted %#v, got %#v", wanted, got)
			}
		})
	}
}
