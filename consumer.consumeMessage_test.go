package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
)

func TestConsumer_ConsumeMessageWhenTimeoutIsReached(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	capi, mock := mock.ConsumerApi()
	mock.Messages([]any{
		api.ErrTimeout,
	})

	sut := &Consumer{
		api: capi,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
	}

	// ACT
	offset, err := sut.consumeMessage(ctx)

	// ASSERT
	t.Run("returns offset", func(t *testing.T) {
		wanted := (*api.Offset)(nil)
		got := offset
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns error", func(t *testing.T) {
		wanted := ErrTimeout
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestConsumer_ConsumeMessageWhenErrorReadingMessage(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	readerr := errors.New("error reading message")

	capi, mock := mock.ConsumerApi()
	mock.Messages([]any{
		readerr,
	})

	sut := &Consumer{
		api: capi,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
	}

	// ACT
	offset, err := sut.consumeMessage(ctx)

	// ASSERT
	t.Run("returns offset", func(t *testing.T) {
		wanted := (*api.Offset)(nil)
		got := offset
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns error", func(t *testing.T) {
		wanted := ApiError{"ReadMessage", readerr}
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestConsumer_ConsumeMessageWhenDecryptReturnsError(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	offset := api.Offset{
		Topic:     "topic",
		Partition: 1,
		Offset:    42,
	}

	capi, mock := mock.ConsumerApi()
	mock.Messages([]any{
		&api.Message{
			Topic:     offset.Topic,
			Partition: &offset.Partition,
			Offset:    &offset.Offset,
		},
	})

	decrypterr := errors.New("error decrypting message")

	sut := &Consumer{
		api: capi,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
		EncryptionHandler: &MockEncryptionHandler{decryptError: decrypterr},
	}

	// ACT
	readoffset, err := sut.consumeMessage(ctx)

	// ASSERT
	t.Run("returns offset", func(t *testing.T) {
		wanted := offset
		got := *readoffset
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns error", func(t *testing.T) {
		wanted := EncryptionHandlerError{error: decrypterr}
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestConsumer_ConsumeMessageWhenHandlerReturnsError(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	offset := api.Offset{
		Topic:     "topic",
		Partition: 1,
		Offset:    42,
	}

	capi, mock := mock.ConsumerApi()
	mock.Messages([]any{
		&api.Message{
			Topic:     offset.Topic,
			Partition: &offset.Partition,
			Offset:    &offset.Offset,
		},
	})

	herr := errors.New("error handling message")

	sut := &Consumer{
		Api: capi,
		Log: Log,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return herr },
			},
		},
		EncryptionHandler: &MockEncryptionHandler{},
	}
	sut.initialise(ctx)

	// ACT
	readoffset, err := sut.consumeMessage(ctx)

	// ASSERT
	t.Run("returns offset", func(t *testing.T) {
		wanted := offset
		got := *readoffset
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns error", func(t *testing.T) {
		wanted := HandlerError{error: herr}
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestConsumer_ConsumeMessageWhenHandlerReturnsFatalError(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	offset := api.Offset{
		Topic:     "topic",
		Partition: 1,
		Offset:    42,
	}

	capi, mock := mock.ConsumerApi()
	mock.Messages([]any{
		&api.Message{
			Topic:     offset.Topic,
			Partition: &offset.Partition,
			Offset:    &offset.Offset,
		},
	})

	herr := errors.New("fatal error")

	sut := &Consumer{
		Api: capi,
		Log: Log,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return FatalHandlerError{"deliberate error", herr} },
			},
		},
		EncryptionHandler: &MockEncryptionHandler{},
	}
	sut.initialise(ctx)

	// ACT
	readoffset, err := sut.consumeMessage(ctx)

	// ASSERT
	t.Run("returns offset", func(t *testing.T) {
		wanted := offset
		got := *readoffset
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns error", func(t *testing.T) {
		wanted := FatalHandlerError{"deliberate error", herr}
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}

func TestConsumer_ConsumeMessageWhenHandlerReturnsNoError(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	offset := api.Offset{
		Topic:     "topic",
		Partition: 1,
		Offset:    42,
	}

	capi, mock := mock.ConsumerApi()
	mock.Messages([]any{
		&api.Message{
			Topic:     offset.Topic,
			Partition: &offset.Partition,
			Offset:    &offset.Offset,
		},
	})

	sut := &Consumer{
		Api: capi,
		Log: Log,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
		EncryptionHandler: &MockEncryptionHandler{},
	}
	sut.initialise(ctx)

	// ACT
	readoffset, err := sut.consumeMessage(ctx)

	// ASSERT
	t.Run("returns offset", func(t *testing.T) {
		wanted := offset
		got := *readoffset
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns error", func(t *testing.T) {
		wanted := (error)(nil)
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}
