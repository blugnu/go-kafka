package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blugnu/kafka/api"
)

func TestConsumer_HandleMessage(t *testing.T) {
	// ARRANGE
	const NoRetryTopic = ""
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	offset := api.Offset{
		Topic:     "topic",
		Partition: 1,
		Offset:    1,
	}

	newmsg := func() *Message {
		return &Message{
			Topic:      offset.Topic,
			Partition:  &offset.Partition,
			Offset:     &offset.Offset,
			Headers:    Headers{},
			RawHeaders: RawHeaders{},
		}
	}

	msgsHandled := 0

	newconsumer := func(args ...any) (*Consumer, *MockDeferralHandler, *MockProducer) {
		msgsHandled = 0
		var handlerError error
		var retryTopic string
		var deadLetterTopic string
		var retryPolicy RetryPolicy
		var deferralHandler = &MockDeferralHandler{}
		var messageProducer = &MockProducer{}

		retryTopicSet := false

		for _, arg := range args {
			switch v := arg.(type) {
			case *MockProducer:
				messageProducer = v
			case *MockDeferralHandler:
				deferralHandler = v
			case *MockRetryPolicy:
				retryPolicy = v
			case string:
				if !retryTopicSet {
					retryTopic = v
					retryTopicSet = true
				} else {
					deadLetterTopic = v
				}
			case error:
				handlerError = v
			}
		}

		return &Consumer{
			log: Log,
			handlers: MessageHandlerMap{
				"topic": MessageHandler{
					Func:            func(context.Context, *Message) error { msgsHandled++; return handlerError },
					DeferralHandler: deferralHandler,
					RetryPolicy:     retryPolicy,
					RetryTopic:      retryTopic,
					DeadLetterTopic: deadLetterTopic,
				},
			},
			MessageProducer: messageProducer,
		}, deferralHandler, messageProducer
	}

	t.Run("message processed successfully", func(t *testing.T) {
		// ARRANGE
		msg := newmsg()

		sut, mockd, _ := newconsumer()

		// ACT
		err := sut.handleMessage(ctx, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("messages handled", func(t *testing.T) {
			wanted := 1
			got := msgsHandled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("deferral handler was not called", func(t *testing.T) {
			wanted := true
			got := !mockd.wasCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("message deferred", func(t *testing.T) {
		// ARRANGE
		msg := newmsg()
		age := 1 * time.Second
		msg.Age = &age
		msg.DeferFor(2 * time.Second)

		sut, mockd, _ := newconsumer()

		// ACT
		err := sut.handleMessage(ctx, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("messages handled", func(t *testing.T) {
			wanted := 0
			got := msgsHandled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("deferral handler was called", func(t *testing.T) {
			wanted := true
			got := mockd.wasCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("produces retry message to RetryTopic", func(t *testing.T) {
		// ARRANGE
		sut, _, mockp := newconsumer(errors.New("handler error"), &MockRetryPolicy{MaxRetries: 1}, "retry")

		// ACT
		err := sut.handleMessage(ctx, newmsg())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("retry messages produced", func(t *testing.T) {
			wanted := 1
			got := len(mockp.Messages["retry"])
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("produces retry message to msg.Topic", func(t *testing.T) {
		// ARRANGE
		sut, _, mockp := newconsumer(errors.New("handler error"), &MockRetryPolicy{MaxRetries: 1})

		// ACT
		err := sut.handleMessage(ctx, newmsg())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("retry messages produced", func(t *testing.T) {
			wanted := 1
			got := len(mockp.Messages["topic"])
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("producing retry message fails", func(t *testing.T) {
		// ARRANGE
		mockp := &MockProducer{error: errors.New("failed to produce message")}

		sut, _, _ := newconsumer(errors.New("handler error"), &MockRetryPolicy{MaxRetries: 1}, mockp)

		// ACT
		err := sut.handleMessage(ctx, newmsg())

		// ASSERT
		t.Run("with error", func(t *testing.T) {
			wanted := ProducerError{mockp.error}
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("retry messages produced", func(t *testing.T) {
			wanted := 0
			got := len(mockp.Messages["topic"])
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("dead-letter when retries exceeded", func(t *testing.T) {
		// ARRANGE
		sut, _, mockp := newconsumer(errors.New("handler error"), &MockRetryPolicy{MaxRetries: 0}, "retry", "dead")

		// ACT
		err := sut.handleMessage(ctx, newmsg())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("retry messages produced", func(t *testing.T) {
			wanted := 0
			got := len(mockp.Messages["retry"])
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("dead-letter messages produced", func(t *testing.T) {
			wanted := 1
			got := len(mockp.Messages["dead"])
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("dead-letter when no retry", func(t *testing.T) {
		// ARRANGE
		sut, _, mockp := newconsumer(errors.New("handler error"), NoRetryTopic, "dead")

		// ACT
		err := sut.handleMessage(ctx, newmsg())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("dead-letter messages produced", func(t *testing.T) {
			wanted := 1
			got := len(mockp.Messages["dead"])
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("producing dead-letter message fails", func(t *testing.T) {
		// ARRANGE
		mockp := &MockProducer{
			error: errors.New("failed to produce message"),
		}
		sut, _, _ := newconsumer(errors.New("handler error"), NoRetryTopic, "dead", mockp)

		// ACT
		err := sut.handleMessage(ctx, newmsg())

		// ASSERT
		t.Run("with error", func(t *testing.T) {
			wanted := ProducerError{mockp.error}
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("dead-letter messages produced", func(t *testing.T) {
			wanted := 0
			got := len(mockp.Messages["dead"])
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("returns handler error if no retry or dead letter topic configured", func(t *testing.T) {
		// ARRANGE
		handlererr := errors.New("handler error")
		sut, _, mockp := newconsumer(handlererr)

		// ACT
		err := sut.handleMessage(ctx, newmsg())

		// ASSERT
		t.Run("with error", func(t *testing.T) {
			wanted := handlererr
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("messages produced", func(t *testing.T) {
			wanted := 0
			got := len(mockp.AllMessages)
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("dead-letter messages produced", func(t *testing.T) {
			wanted := 0
			got := len(mockp.Messages["dead"])
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}
