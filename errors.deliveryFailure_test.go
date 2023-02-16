package kafka

import (
	"errors"
	"testing"
)

func TestDeliveryFailure(t *testing.T) {
	// ARRANGE
	dferr := errors.New("delivery failure")
	sut := DeliveryFailure{dferr}

	t.Run("error string", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "delivery failed: delivery failure"
		got := s
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("is", func(t *testing.T) {
		testcases := []struct {
			name string
			error
			result bool
		}{
			{"same error", DeliveryFailure{dferr}, true},
			{"different error", DeliveryFailure{errors.New("other error")}, false},
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
