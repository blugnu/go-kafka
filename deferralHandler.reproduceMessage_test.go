package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/blugnu/kafka/api/mock"
)

func TestDeferralHandler_ReproduceMessage(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	partition := int32(1)
	offset := int64(0)
	age := time.Duration(1 * time.Second)
	msg := &Message{
		Topic:     "topic",
		Partition: &partition,
		Offset:    &offset,
		Age:       &age,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	mockproducer := &MockProducer{}

	capi, mockconsumer := mock.ConsumerApi()
	consumer := &Consumer{
		api:             capi,
		log:             Log,
		MessageProducer: mockproducer,
	}

	sut := ReproduceMessage{}

	t.Run("when deferral has expired", func(t *testing.T) {
		// ARRANGE
		mockconsumer.Reset()
		defer mockconsumer.Reset()
		defer mockproducer.Reset()

		msg.DeferFor(0)

		// ACT
		err := sut.Defer(ctx, consumer, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("produces message", func(t *testing.T) {
			// ARRANGE
			msg := mockproducer.AllMessages[0]

			wanted := &Message{
				Topic:      "topic",
				Partition:  &partition,
				Offset:     &offset,
				Key:        []byte("key"),
				Value:      []byte("value"),
				Headers:    Headers{},
				RawHeaders: RawHeaders{},
			}
			got := msg
			if !wanted.Equal(got) {
				t.Errorf("wanted %[1]T (%[1]v), got %[2]T (%[2]v)", wanted, got)
			}
		})
	})

	t.Run("when deferral has not expired", func(t *testing.T) {
		// ARRANGE
		mockconsumer.Reset()
		defer mockconsumer.Reset()
		defer mockproducer.Reset()

		msg.DeferFor(2 * time.Second)

		// ACT
		err := sut.Defer(ctx, consumer, msg)

		// ASSERT
		t.Run("produces message", func(t *testing.T) {
			// ARRANGE
			msg := mockproducer.AllMessages[0]

			// ASSERT
			wanted := &Message{
				Topic:      "topic",
				Partition:  &partition,
				Offset:     &offset,
				Key:        []byte("key"),
				Value:      []byte("value"),
				Headers:    Headers{"blugnu/kafka:deferred": "1s"},
				RawHeaders: RawHeaders{"blugnu/kafka:deferred": []byte("1s")},
			}
			got := msg
			if !wanted.Equal(got) {
				t.Errorf("wanted %[1]T (%[1]v), got %[2]T (%[2]v)", wanted, got)
			}
		})

		t.Run("returns error", func(t *testing.T) {
			wanted := (error)(nil)
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})
}
