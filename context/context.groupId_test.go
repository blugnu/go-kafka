package context

import (
	"context"
	"testing"
)

func TestGroupId(t *testing.T) {
	ctx := context.Background()

	t.Run("when not set", func(t *testing.T) {
		// ACT
		v, b := GroupId(ctx)

		// ASSERT
		t.Run("returns group id", func(t *testing.T) {
			wanted := ""
			got := v
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("returns isSet indicator", func(t *testing.T) {
			wanted := false
			got := b
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("when set", func(t *testing.T) {
		// ARRANGE
		ctx := context.WithValue(ctx, groupIdKey, "id")

		// ACT
		v, b := GroupId(ctx)

		// ASSERT
		t.Run("returns group id", func(t *testing.T) {
			wanted := "id"
			got := v
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("returns isSet indicator", func(t *testing.T) {
			wanted := true
			got := b
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}

func TestWithGroupId(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	// ACT
	sut := WithGroupId(ctx, "id")

	// ASSERT
	wanted := "id"
	got := sut.Value(groupIdKey)
	if wanted != got {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}
