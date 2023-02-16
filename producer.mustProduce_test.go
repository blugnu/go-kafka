package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestMustProduce(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	msg := &Message{
		Topic: "topic",
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	mockp := &MockProducer{}
	DefaultProducer = mockp
	defer func() { DefaultProducer = nil }()

	// ACT
	_, err := MustProduce(ctx, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("produces using DefaultProducer", func(t *testing.T) {
		t.Run("number of messages produced", func(t *testing.T) {
			wanted := 1
			got := len(mockp.AllMessages)
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("message produced", func(t *testing.T) {
			ptn := int32(1)
			off := int64(0)
			wanted := &Message{
				Topic:     "topic",
				Partition: &ptn,
				Offset:    &off,
				Key:       []byte("key"),
				Value:     []byte("value"),
			}
			got := mockp.AllMessages[0]
			if !wanted.Equal(got) {
				t.Errorf("wanted %#v, got %#v", wanted, got)
			}
		})
	})
}

func TestProducer_MustProduce(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	newproducer := func(args ...any) *Producer {
		producer := &Producer{
			log:   Log,
			state: psConnected,
		}

		for _, arg := range args {
			switch v := arg.(type) {
			case *MockEncryptionHandler:
				producer.EncryptionHandler = v
			case api.Producer:
				producer.api = v
			case producerState:
				producer.state = v
			}
		}

		if producer.api == nil {
			mockapi, _ := mock.ProducerApi()
			producer.api = mockapi
		}

		if producer.EncryptionHandler == nil {
			producer.EncryptionHandler = &MockEncryptionHandler{}
		}

		return producer
	}
	msg := &Message{}

	t.Run("when not connected", func(t *testing.T) {
		// ARRANGE
		sut := newproducer(psInitialising)

		// ACT
		_, err := sut.MustProduce(ctx, msg)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ErrNotConnected
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("with no TopicId", func(t *testing.T) {
		// ARRANGE
		sut := newproducer()

		// ACT
		_, err := sut.MustProduce(ctx, msg)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ErrNoTopicId
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	// ARRANGE
	msg.Topic = "topic"

	t.Run("when encryption fails", func(t *testing.T) {
		// ARRANGE
		eerr := errors.New("encryption error")
		encryptionHandler := &MockEncryptionHandler{encryptError: eerr}

		sut := newproducer(encryptionHandler)

		// ACT
		_, err := sut.MustProduce(ctx, msg)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := EncryptionHandlerError{eerr}
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("when api.Produce fails", func(t *testing.T) {
		// ARRANGE
		apierr := errors.New("api error")
		papi, mockapi := mock.ProducerApi()
		mockapi.Funcs.Produce = func(m *api.Message, c chan confluent.Event) error { return apierr }

		sut := newproducer(papi)

		// ACT
		_, err := sut.MustProduce(ctx, msg)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ApiError{"Produce", apierr}
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("when delivery fails", func(t *testing.T) {
		// ARRANGE
		derr := confluent.NewError(confluent.ErrAllBrokersDown, "brokers down", true)
		papi, mockapi := mock.ProducerApi()
		mockapi.Funcs.Produce = func(msg *api.Message, dc chan confluent.Event) error {
			go func() {
				dc <- derr
			}()
			return nil
		}

		sut := newproducer(papi)

		// ACT
		offset, err := sut.MustProduce(ctx, msg)

		// ASSERT
		t.Run("returns offset", func(t *testing.T) {
			wanted := (*api.Offset)(nil)
			got := offset
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("returns error", func(t *testing.T) {
			wanted := DeliveryFailure{derr}
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("producing message successfully", func(t *testing.T) {
		// ARRANGE
		encryptionHandler := &MockEncryptionHandler{}
		papi, mockapi := mock.ProducerApi()

		sut := newproducer(encryptionHandler, papi)

		// ACT
		offset, err := sut.MustProduce(ctx, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("message is encrypted", func(t *testing.T) {
			wanted := true
			got := encryptionHandler.encrypt.wasCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("produces message", func(t *testing.T) {
			wanted := true
			got := mockapi.ProduceWasCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("returns offset", func(t *testing.T) {
			wanted := &api.Offset{Topic: "topic", Partition: 1, Offset: 1}
			got := offset
			if *wanted != *got {
				t.Errorf("wanted %v, got %v", *wanted, *got)
			}
		})
	})

}
