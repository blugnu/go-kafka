package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
)

func TestReader_Connect(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	defer InstallTestLogger(ctx)()

	capi, mock := mock.ConsumerApi()

	newreader := func(args ...any) *Reader {
		return &Reader{
			log:    Log,
			Api:    capi,
			Topics: []string{"topic"},
		}
	}

	t.Run("when no topics configured", func(t *testing.T) {
		// ARRANGE
		defer mock.Reset()

		sut := newreader()
		sut.Topics = nil

		// ACT
		err := sut.connect(ctx)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ConfigurationError{error: ErrNoTopics}
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		defer mock.Reset()

		sut := newreader()

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

		sut := newreader()

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
