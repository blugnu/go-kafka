package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
)

func TestConsumer_Subscribe(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	defer InstallTestLogger(ctx)()

	subscribeCalled := false

	capi, mock := mock.ConsumerApi()

	sut := &Consumer{
		api: capi,
		log: Log,
	}

	t.Run("when api.Subscribe is successful", func(t *testing.T) {
		// ARRANGE
		mock.Funcs.Subscribe = func([]string, api.RebalanceEventHandler) error { subscribeCalled = true; return nil }

		// ACT
		err := sut.subscribe(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("calls api.Subscribe", func(t *testing.T) {
			wanted := true
			got := subscribeCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("when api.Subscribe returns error", func(t *testing.T) {
		// ARRANGE
		apierr := errors.New("Api Error")
		mock.Funcs.Subscribe = func([]string, api.RebalanceEventHandler) error { return apierr }

		// ACT
		err := sut.subscribe(ctx)

		// ASSERT
		t.Run("returns api error", func(t *testing.T) {
			wanted := ApiError{"Subscribe", apierr}
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})
}

func TestConsumer_SubscribesToTopicsandRetryTopics(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	defer InstallTestLogger(ctx)()

	var subscribedTopics []string

	capi, mock := mock.ConsumerApi()

	mock.Funcs.Subscribe = func(topics []string, _ api.RebalanceEventHandler) error {
		subscribedTopics = append(subscribedTopics, topics...)
		return nil
	}

	sut := &Consumer{
		api: capi,
		log: Log,
		MessageHandlers: MessageHandlerMap{
			"topic": {
				Func:        func(context.Context, *Message) error { return nil },
				RetryTopic:  "retry",
				RetryPolicy: &LinearRetryPolicy{},
			},
		},
	}

	// ACT
	err := sut.subscribe(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("subscribes to expected topics", func(t *testing.T) {
		wanted := []string{"topic", "retry"}
		got := subscribedTopics
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %v\ngot    %v", wanted, got)
		}
	})
}
