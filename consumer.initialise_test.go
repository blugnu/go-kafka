package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/api/confluent"
	"github.com/blugnu/logger"
)

func TestThatConsumer_InitialisedWithNoMessageHandlers(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{}

	// ACT
	err := sut.initialise(ctx)

	// ASSERT
	t.Run("error returned", func(t *testing.T) {
		wanted := ConfigurationError{error: ErrNoMessageHandlers}
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestThatConsumer_InitialisedWithNoConfiguration(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
	}

	defer InstallTestLogger(ctx)()

	// set sentinels in global/default configuration (restoring defaults when done)
	oencryption := Encryption
	defer func() { Encryption = oencryption }()
	Encryption = &MockEncryptionHandler{}

	// ACT
	err := sut.initialise(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT base logger configuration
	t.Run("configures base logger", func(t *testing.T) {
		t.Run("of expected type", func(t *testing.T) {
			wanted := &logger.Base{}
			got := sut.log
			if got == nil {
				t.Errorf("\nwanted %T\ngot    nil", wanted)
			}
		})
		t.Run("with expected context", func(t *testing.T) {
			wanted := ctx
			got := sut.log.Context
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	// ASSERT default Api configuration
	t.Run("configures confluent Consumer api", func(t *testing.T) {
		wanted := &confluent.Consumer{}
		got := sut.api
		if got == nil {
			t.Errorf("\nwanted %T\ngot    nil", wanted)
		}
	})

	// ASSERT default EncryptionHandler configuration
	t.Run("configures default EncryptionHandler", func(t *testing.T) {
		wanted := Encryption
		got := sut.EncryptionHandler
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	// ASSERT handlers
	t.Run("configures handlers", func(t *testing.T) {
		wanted := 1
		got := len(sut.handlers)
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func TestThatConsumer_InitialisedWithNoDefaultEncryptionHandler(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
	}

	defer InstallTestLogger(ctx)()

	// set sentinels in global/default configuration (restoring defaults when done)
	oencryption := Encryption
	defer func() { Encryption = oencryption }()
	Encryption = nil

	// ACT
	err := sut.initialise(ctx)

	// ASSERT
	t.Run("returns error", func(t *testing.T) {
		wanted := ConfigurationError{Topic: "", error: ErrNoEncryptionHandler}
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestThatConsumer_InitialisedWithMessageHandlerWithNoFunc(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{
		MessageHandlers: MessageHandlerMap{
			"topic": {},
		},
	}

	defer InstallTestLogger(ctx)()

	// ACT
	err := sut.initialise(ctx)

	// ASSERT
	t.Run("returns ConfigurationError", func(t *testing.T) {
		wanted := ConfigurationError{Topic: "topic", error: ErrNoHandlerFunc}
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestThatConsumer_InitialisedWithMessageHandlerWithFuncConfiguresExpectedHandlers(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{
		MessageHandlers: MessageHandlerMap{
			"topic": {Func: func(context.Context, *Message) error { return nil }},
		},
	}

	defer InstallTestLogger(ctx)()

	// ACT
	err := sut.initialise(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := MessageHandlerMap{
		"topic": sut.MessageHandlers["topic"],
	}
	got := sut.handlers
	if !wanted.Equals(got) {
		t.Errorf("\nwanted %v\ngot    %v", wanted, got)
	}
}

func TestThatConsumer_InitialisedWithMessageHandlerWithRetryTopicButNoRetryPolicy(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{
		MessageHandlers: MessageHandlerMap{
			"topic": {
				Func:       func(context.Context, *Message) error { return nil },
				RetryTopic: "retry",
			},
		},
	}

	defer InstallTestLogger(ctx)()

	// ACT
	err := sut.initialise(ctx)

	// ASSERT
	t.Run("returns ConfigurationError", func(t *testing.T) {
		wanted := ConfigurationError{Topic: "topic", error: ErrNoRetryPolicy}
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestThatConsumer_InitialisedWithMessageHandlerWithRetryTopicRequiresDeferralHandler(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{
		MessageHandlers: MessageHandlerMap{
			"topic": {
				Func:        func(context.Context, *Message) error { return nil },
				RetryTopic:  "retry",
				RetryPolicy: &LinearRetryPolicy{},
			},
		},
	}

	defer InstallTestLogger(ctx)()

	// ACT
	err := sut.initialise(ctx)

	// ASSERT
	t.Run("returns error", func(t *testing.T) {
		wanted := ConfigurationError{Topic: "topic", error: ErrNoDeferralHandler}
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestThatConsumer_InitialisedWithMessageHandlerWithRetryTopicRequiresMessageProducer(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{
		MessageHandlers: MessageHandlerMap{
			"topic": {
				Func:        func(context.Context, *Message) error { return nil },
				RetryTopic:  "retry",
				RetryPolicy: &LinearRetryPolicy{},
			},
		},
		DeferralHandler: &MockDeferralHandler{},
	}

	defer InstallTestLogger(ctx)()

	// ACT
	err := sut.initialise(ctx)

	// ASSERT
	t.Run("returns error", func(t *testing.T) {
		wanted := ConfigurationError{error: ErrNoMessageProducer}
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestThatConsumer_InitialisedWithMessageHandlerWithRetryTopicConfiguresRetryHandler(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{
		MessageHandlers: MessageHandlerMap{
			"topic": {
				Func:            func(context.Context, *Message) error { return nil },
				RetryTopic:      "retry",
				RetryPolicy:     &LinearRetryPolicy{},
				DeferralHandler: &MockDeferralHandler{},
			},
		},
		MessageProducer: &MockProducer{},
	}

	defer InstallTestLogger(ctx)()

	// ACT
	err := sut.initialise(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := MessageHandlerMap{
		"topic": sut.MessageHandlers["topic"],
		"retry": sut.MessageHandlers["topic"],
	}
	got := sut.handlers
	if !wanted.Equals(got) {
		t.Errorf("\nwanted %v\ngot    %v", wanted, got)
	}
}
