package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
)

func TestConsumer_Connect(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	defer InstallTestLogger(ctx)()

	capi, mock := mock.ConsumerApi()

	sut := &Consumer{
		api: capi,
		log: Log,
	}

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		// createCalled := false
		// mock.Funcs.Create = func(cfg *api.ConfigMap) error { createCalled = true; return nil }
		defer mock.Reset()

		// ACT
		err := sut.connect(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("calls api.Create", func(t *testing.T) {
			wanted := true
			got := mock.CreateWasCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("when api.Create fails", func(t *testing.T) {
		// ARRANGE
		apierr := errors.New("Api Error")
		mock.Funcs.Create = func(cfg *api.ConfigMap) error { return apierr }
		defer mock.Reset()

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

	// t.Run("when noConnection is true", func(t *testing.T) {
	// 	// ARRANGE
	// 	sut.noConnection = true

	// 	createCalled := false
	// 	mock.Funcs.Create = func(cfg *api.ConfigMap) error { createCalled = true; return nil }

	// 	// ACT
	// 	err := sut.connect(ctx)
	// 	if err != nil {
	// 		t.Fatalf("unexpected error: %v", err)
	// 	}

	// 	// ASSERT
	// 	t.Run("does not call api.Create", func(t *testing.T) {
	// 		wanted := false
	// 		got := createCalled
	// 		if wanted != got {
	// 			t.Errorf("wanted %v, got %v", wanted, got)
	// 		}
	// 	})
	// })
}
