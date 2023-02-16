package context

import (
	"context"
	"testing"
)

func TestMessageProduced(t *testing.T) {
	ctx := context.Background()

	t.Run("when not set", func(t *testing.T) {
		// ACT
		v := MessageProduced(ctx)

		// ASSERT
		t.Run("returns", func(t *testing.T) {
			wanted := (*MessageSummary)(nil)
			got := v
			if wanted != got {
				t.Errorf("wanted %#v, got %#v", wanted, got)
			}
		})
	})

	t.Run("when set", func(t *testing.T) {
		// ARRANGE
		sum := &MessageSummary{}
		ctx := context.WithValue(ctx, messageProducedKey, sum)

		// ACT
		v := MessageProduced(ctx)

		// ASSERT
		t.Run("returns", func(t *testing.T) {
			wanted := sum
			got := v
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}

func TestWithMessageProduced(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sum := &MessageSummary{}

	// ACT
	sut := WithMessageProduced(ctx, sum)

	// ASSERT
	wanted := sum
	got := sut.Value(messageProducedKey)
	if wanted != got {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}
