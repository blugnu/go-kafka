package kafka

import (
	"fmt"
	"testing"
	"time"
)

func TestLinearRetryPolicy(t *testing.T) {
	// ARRANGE

	testcases := []struct {
		wait   time.Duration
		max    int
		result string
	}{
		{wait: 2 * time.Second, max: 2, result: "Linear: Wait 2s (max 2 retries)"},
		{wait: 120 * time.Second, max: 1, result: "Linear: Wait 2m0s (1 retry)"},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("wait: %s, max: %d ", tc.wait, tc.max), func(t *testing.T) {
			// ARRANGE
			sut := &LinearRetryPolicy{Wait: tc.wait, MaxRetries: tc.max}

			t.Run("stringer", func(t *testing.T) {
				// ACT
				got := sut.String()

				// ASSERT
				wanted := tc.result
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})

			t.Run("Delay returns Wait", func(t *testing.T) {
				// ACT
				wanted := tc.wait
				got := sut.RetryDelay(nil)

				// ASSERT
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})
		})
	}
}

func TestLinear_ShouldRetry(t *testing.T) {
	// ARRANGE
	sut := &LinearRetryPolicy{}

	testcases := []struct {
		name   string
		max    int
		retry  int
		result bool
	}{
		{name: "within retry limit", max: 2, retry: 1, result: true},
		{name: "within retry limit", max: 2, retry: 2, result: true},
		{name: "retry limit exceeded", max: 2, retry: 3, result: false},
		{name: "unlimited retries, 1st retry", max: -1, retry: 1, result: true},
		{name: "unlimited retries, 99th retry", max: -1, retry: 99, result: true},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			sut.MaxRetries = tc.max
			msg := &Message{}
			msg.setRetryHeaders(1, tc.retry)

			// ACT
			got := sut.ShouldRetry(msg)

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}
}
