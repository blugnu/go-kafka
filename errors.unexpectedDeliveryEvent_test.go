package kafka

import (
	"errors"
	"testing"
)

func TestUnexpectedDeliveryEvent(t *testing.T) {
	// ARRANGE
	sut := UnexpectedDeliveryEvent{MockEvent("event")}

	t.Run("error string", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "unexpected delivery event (kafka.MockEvent): event"
		got := s
		if wanted != got {
			t.Errorf("wanted %q, got %q", wanted, got)
		}
	})

	t.Run("is", func(t *testing.T) {
		testcases := []struct {
			name string
			error
			result bool
		}{
			{name: "same event", error: UnexpectedDeliveryEvent{MockEvent("event")}, result: true},
			{name: "different event", error: UnexpectedDeliveryEvent{MockEvent("other event")}, result: false},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				wanted := tc.result
				got := errors.Is(sut, tc.error)

				// ASSERT
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})
		}
	})
}
