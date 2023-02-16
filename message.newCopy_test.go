package kafka

import (
	"testing"
	"time"
)

func TestMessage_NewCopy(t *testing.T) {
	// ARRANGE
	ptn := int32(1)
	off := int64(1)
	ts := time.Date(2010, 9, 8, 7, 6, 5, 0, time.UTC)
	age := 4 * time.Second

	sut := &Message{
		Topic:     "topic",
		Partition: &ptn,
		Offset:    &off,
		Key:       []byte("key"),
		Value:     []byte("value"),
		Timestamp: &ts,
		Age:       &age,
	}
	sut.DeferFor(2 * time.Second)
	sut.SetHeader("preserved", "header")

	// ACT
	copy := sut.NewCopy()

	// ASSERT
	wanted := &Message{
		Topic:      "",
		Partition:  nil,
		Offset:     nil,
		Key:        []byte("key"),
		Value:      []byte("value"),
		Timestamp:  nil,
		Age:        nil,
		Headers:    Headers{"preserved": "header"},
		RawHeaders: RawHeaders{"preserved": []byte("header")},
	}
	got := copy
	if !wanted.Equal(got) {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}
