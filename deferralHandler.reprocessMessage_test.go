package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blugnu/kafka/api/mock"
)

func TestDeferralHandler_ReprocessMessage(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	partition := int32(1)
	offset := int64(1)
	msg := &Message{
		Topic:     "topic",
		Partition: &partition,
		Offset:    &offset,
	}
	capi, mock := mock.ConsumerApi()
	consumer := &Consumer{api: capi, log: Log}

	sut := ReprocessMessage{}

	t.Run("when api.Seek returns error", func(t *testing.T) {
		// ARRANGE
		apierr := errors.New("api error")
		mock.Funcs.Seek = func(o *Offset, d time.Duration) error { return apierr }
		defer mock.Reset()

		// ACT
		err := sut.Defer(ctx, consumer, msg)

		// ASSERT
		wanted := ApiError{"Seek", apierr}
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		defer mock.Reset()

		// ACT
		err := sut.Defer(ctx, consumer, msg)

		// ASSERT
		wanted := ErrReprocessMessage
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}
