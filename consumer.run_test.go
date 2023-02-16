package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
)

func TestConsumer_RunWhenNotInInitialisingState(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{
		state: csCloseFailed,
	}

	// ACT
	err := sut.Run(ctx)

	// ASSERT
	wanted := InvalidStateError{operation: "Run", state: sut.state.String()}
	got := err
	if !errors.Is(got, wanted) {
		t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
	}
}

func TestConsumer_RunWhenConfigurationIsInvalid(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &Consumer{}

	// ACT
	err := sut.Run(ctx)

	// ASSERT
	t.Run("returns error", func(t *testing.T) {
		wanted := ConfigurationError{error: ErrNoMessageHandlers}
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})

	t.Run("consumer errors", func(t *testing.T) {
		{
			wanted := 1
			got := len(sut.errors)
			if wanted != got {
				t.Fatalf("wanted %v, got %v", wanted, got)
			}
		}
		{
			wanted := err
			got := sut.errors[0]
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		}
	})

	t.Run("consumer state", func(t *testing.T) {
		wanted := csInitialiseFailed
		got := sut.state
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func TestConsumer_RunWhenConnectionFails(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	capi, mock := mock.ConsumerApi()
	apierr := errors.New("create failed")
	mock.Funcs.Create = func(*api.ConfigMap) error { return apierr }

	sut := &Consumer{
		Api: capi,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
	}

	// ACT
	err := sut.Run(ctx)

	// ASSERT
	t.Run("returns error", func(t *testing.T) {
		wanted := ApiError{"Create", apierr}
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})

	t.Run("consumer errors", func(t *testing.T) {
		{
			wanted := 1
			got := len(sut.errors)
			if wanted != got {
				t.Fatalf("wanted %v, got %v", wanted, got)
			}
		}
		{
			wanted := err
			got := sut.errors[0]
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		}
	})

	t.Run("consumer state", func(t *testing.T) {
		wanted := csConnectFailed
		got := sut.state
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func TestConsumer_RunWhenSubscribeFails(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	capi, mock := mock.ConsumerApi()
	apierr := errors.New("subscribe failed")
	mock.Funcs.Subscribe = func([]string, api.RebalanceEventHandler) error { return apierr }

	sut := &Consumer{
		Api: capi,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
	}

	// ACT
	err := sut.Run(ctx)

	// ASSERT
	t.Run("returns error", func(t *testing.T) {
		wanted := ApiError{"Subscribe", apierr}
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})

	t.Run("consumer errors", func(t *testing.T) {
		{
			wanted := 1
			got := len(sut.errors)
			if wanted != got {
				t.Fatalf("wanted %v, got %v", wanted, got)
			}
		}
		{
			wanted := err
			got := sut.errors[0]
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		}
	})

	t.Run("consumer state", func(t *testing.T) {
		wanted := csSubscribeFailed
		got := sut.state
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func TestConsumer_RunWhenConnectedAndSubscribed(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	capi, _ := mock.ConsumerApi()

	sut := &Consumer{
		Api: capi,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
	}
	// consumeMessagesCalled := false
	// sut._consumeMessages = func(context.Context) {
	// 	consumeMessagesCalled = true
	// }

	// ACT
	err := sut.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT

	// The mock api terminates the consumer when mocked messages are exhausted.
	// Since we have not mocked any messages for this test, the consumer will start
	// running then immediately halt.
	//
	// We'll wait a maximum 1/2 second for the consumer to reach the closed state,
	// confirming that the message loop ran and terminated cleanly.
	tm := time.Now()
	for sut.state != csClosed {
		if time.Since(tm) > 500*time.Millisecond {
			t.Fatalf("consumer did not run")
			break
		}
	}
}
