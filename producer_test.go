package kafka

import (
	"context"
	"testing"
)

func Test_MustProduce(t *testing.T) {

	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	msg := &Message{
		Topic: "topic",
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	t.Run("when no DefaultProducer is configured", func(t *testing.T) {
		// ACT
		offset, err := MustProduce(ctx, msg)

		// ASSERT
		t.Run("returns nil offset", func(t *testing.T) {
			wanted := (*Offset)(nil)
			got := offset
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("returns error", func(t *testing.T) {
			wanted := ErrNoDefaultProducer
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	// ARRANGE
	mockp := &MockProducer{}
	DefaultProducer = mockp

	// ACT
	offset, err := MustProduce(ctx, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("returns offset", func(t *testing.T) {
		wanted := &Offset{Topic: "topic", Partition: 1, Offset: 0}
		got := offset
		if *wanted != *got {
			t.Errorf("wanted %#v, got %#v", wanted, got)
		}
	})
}
