package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
	"github.com/blugnu/kafka/set"
)

func TestReader_ReadAll(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	capi, mock := mock.ConsumerApi()

	newreader := func() *Reader {
		return &Reader{
			Api:    capi,
			Topics: []string{"topica", "topicb"},
		}
	}

	t.Run("when no topics configured", func(t *testing.T) {
		// ARRANGE
		sut := newreader()
		sut.Topics = nil

		// ACT
		_, err := sut.ReadAll(ctx, 0)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ConfigurationError{error: ErrNoTopics}
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("when connect fails", func(t *testing.T) {
		// ARRANGE
		apierr := errors.New("api error")
		mock.Funcs.Create = func(cm *api.ConfigMap) error { return apierr }
		defer mock.Reset()

		sut := newreader()

		// ACT
		_, err := sut.ReadAll(ctx, 0)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ApiError{"Create", apierr}
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("when api.Assign fails", func(t *testing.T) {
		// ARRANGE
		apierr := errors.New("api error")
		mock.Funcs.Assign = func(s []string) error { return apierr }
		defer mock.Reset()

		sut := newreader()

		// ACT
		_, err := sut.ReadAll(ctx, 0)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ApiError{"Assign", apierr}
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("when connected", func(t *testing.T) {
		// ARRANGE
		msgerr := errors.New("no messages")
		mock.Messages([]any{msgerr})
		defer mock.Reset()

		sut := newreader()

		// ACT
		_, err := sut.ReadAll(ctx, 0)
		if err != msgerr {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("assigns topics", func(t *testing.T) {
			wanted := []string{"topica", "topicb"}
			got := mock.TopicsAssigned
			if !set.FromSlice(wanted).Equal(set.FromSlice(got)) {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("unassigns topics", func(t *testing.T) {
			wanted := true
			got := mock.AssignWasCalledWithNil
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("when no more messages", func(t *testing.T) {
		// ARRANGE
		srcmsg := &api.Message{Topic: "topic"}
		mock.Messages([]any{srcmsg, api.ErrTimeout})
		defer mock.Reset()

		sut := newreader()

		// ACT
		msgs, err := sut.ReadAll(ctx, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("returns messages", func(t *testing.T) {
			wanted := 1
			got := len(msgs)
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
			t.Run("from topics", func(t *testing.T) {
				wanted := fromApiMessage(srcmsg)
				got := msgs[0]
				if !wanted.Equal(got) {
					t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
				}
			})
		})

		t.Run("unassigns topics", func(t *testing.T) {
			wanted := true
			got := mock.AssignWasCalledWithNil
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}
