package context

import (
	"context"
	"testing"
)

func TestRetryPolicy(t *testing.T) {
	ctx := context.Background()

	t.Run("when not set", func(t *testing.T) {
		// ACT
		v := RetryPolicy(ctx)

		// ASSERT
		t.Run("returns", func(t *testing.T) {
			wanted := ""
			got := v
			if wanted != got {
				t.Errorf("wanted %#v, got %#v", wanted, got)
			}
		})
	})

	t.Run("when set", func(t *testing.T) {
		// ARRANGE
		ctx := context.WithValue(ctx, retryPolicyKey, "policy")

		// ACT
		v := RetryPolicy(ctx)

		// ASSERT
		t.Run("returns", func(t *testing.T) {
			wanted := "policy"
			got := v
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}

func TestWithRetryPolicy(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	// ACT
	sut := WithRetryPolicy(ctx, "policy")

	// ASSERT
	wanted := "policy"
	got := sut.Value(retryPolicyKey)
	if wanted != got {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}
