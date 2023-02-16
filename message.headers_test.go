package kafka

import "testing"

func TestMessageHeaders_SetHeader(t *testing.T) {
	// ARRANGE
	msg := &Message{}

	// ACT
	msg.SetHeader("key", "value")

	// ASSERT
	wanted := &Message{
		Headers:    Headers{"key": "value"},
		RawHeaders: RawHeaders{"key": []byte("value")},
	}
	got := msg
	if !wanted.Equal(got) {
		t.Errorf("wanted %#v, got %#v", wanted, got)
	}
}

func TestMessageHeaders_SetRawHeader(t *testing.T) {
	// ARRANGE
	msg := &Message{}

	// ACT
	msg.SetRawHeader("key", []byte("value"))

	// ASSERT
	wanted := &Message{
		Headers:    Headers{"key": "value"},
		RawHeaders: RawHeaders{"key": []byte("value")},
	}
	got := msg
	if !wanted.Equal(got) {
		t.Errorf("wanted %#v, got %#v", wanted, got)
	}
}
