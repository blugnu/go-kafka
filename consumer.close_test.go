package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/api/mock"
)

func TestConsumer_Close(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	capi, mockapi := mock.ConsumerApi()

	sut := &Consumer{
		api: capi,
		log: Log,
	}

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		closeCalled := false
		mockapi.Funcs.Close = func() error { closeCalled = true; return nil }

		// ACT
		err := sut.close()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("calls api.Close", func(t *testing.T) {
			wanted := true
			got := closeCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("consumer state", func(t *testing.T) {
			wanted := csClosed
			got := sut.state
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("fails", func(t *testing.T) {
		// ARRANGE
		apierr := errors.New("close error")
		mockapi.Funcs.Close = func() error { return apierr }

		// ACT
		err := sut.close()

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ApiError{"Close", apierr}
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("consumer state", func(t *testing.T) {
			wanted := csCloseFailed
			got := sut.state
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}
