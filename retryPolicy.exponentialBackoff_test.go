package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/blugnu/kafka/set"
)

func TestExponentialBackoffRetryPolicy(t *testing.T) {
	// ARRANGE

	testcases := []struct {
		initialWait time.Duration
		maxWait     time.Duration
		maxRetries  int
		delays      []int // slice containing all expected delays, in seconds
		string
	}{
		{initialWait: 2 * time.Second, maxWait: 4 * time.Second, maxRetries: 5, string: "ExponentialBackoff: Wait 2s-4s (max 5 retries)", delays: []int{2, 4, 4, 4, 4}},
		{initialWait: 2 * time.Second, maxWait: 3 * time.Second, maxRetries: 5, string: "ExponentialBackoff: Wait 2s-3s (max 5 retries)", delays: []int{2, 3, 3, 3, 3}},
		{initialWait: 120 * time.Second, maxRetries: 1, string: "ExponentialBackoff: Wait 2m0s (1 retry)", delays: []int{120}},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("initial wait: %s, max wait: %s, max retries: %d ", tc.initialWait, tc.maxWait, tc.maxRetries), func(t *testing.T) {
			// ARRANGE
			sut := &ExponentialBackoffRetryPolicy{InitialWait: tc.initialWait, MaxWait: tc.maxWait, MaxRetries: tc.maxRetries}

			t.Run("stringer", func(t *testing.T) {
				// ACT
				got := sut.String()

				// ASSERT
				wanted := tc.string
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})

			t.Run("calculated delays", func(t *testing.T) {
				// ARRANGE
				msg := &Message{}

				// ACT
				got := []time.Duration{}
				for i := 1; i <= tc.maxRetries; i++ {
					msg.setRetryHeaders(1, i)
					got = append(got, sut.RetryDelay(msg))
				}

				// ASSERT
				wanted := []time.Duration{}
				for _, i := range tc.delays {
					wanted = append(wanted, time.Duration(i)*time.Second)
				}
				if !set.FromSlice(wanted).Equal(set.FromSlice(got)) {
					t.Errorf("wanted %#v, got %#v", wanted, got)
				}
			})
		})
	}
}

func TestExponentialBackOff_ShouldRetry(t *testing.T) {
	// ARRANGE
	sut := &ExponentialBackoffRetryPolicy{}

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
