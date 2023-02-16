package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
)

func TestProducer_Connect(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	t.Run("when not initialising", func(t *testing.T) {
		// ARRANGE
		sut := &Producer{state: psConnected}

		// ACT
		err := sut.Connect(ctx)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := InvalidStateError{operation: "Connect", state: "psConnected"}
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("when api.Connect fails", func(t *testing.T) {
		// ARRANGE
		apierr := errors.New("api error")

		papi, mockp := mock.ProducerApi()
		mockp.Funcs.Create = func(cm *api.ConfigMap) error { return apierr }

		defer InstallTestLogger(ctx)()

		sut := &Producer{Log: Log, Api: papi}

		// ACT
		err := sut.Connect(ctx)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ApiError{"Create", apierr}
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})
}

func TestProducer_connect(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	defer InstallTestLogger(ctx)()

	capi, mock := mock.ProducerApi()

	sut := &Producer{
		api: capi,
		log: Log,
	}

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		createCalled := false
		mock.Funcs.Create = func(cfg *api.ConfigMap) error { createCalled = true; return nil }

		// ACT
		err := sut.connect(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("calls api.Create", func(t *testing.T) {
			wanted := true
			got := createCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("when api.Create fails", func(t *testing.T) {
		// ARRANGE
		apierr := errors.New("Api Error")
		mock.Funcs.Create = func(cfg *api.ConfigMap) error { return apierr }

		// ACT
		err := sut.connect(ctx)

		// ASSERT
		t.Run("returns api error", func(t *testing.T) {
			wanted := ApiError{"Create", apierr}
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})
}
