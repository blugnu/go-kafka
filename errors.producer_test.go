package kafka

import (
	"errors"
	"testing"
)

func TestProducerError(t *testing.T) {
	// ARRANGE
	herr := errors.New("producer error")
	sut := ProducerError{herr}

	t.Run("error string", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "producer error: producer error"
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
			{name: "same error", error: ProducerError{herr}, result: true},
			{name: "different error", error: ProducerError{errors.New("other error")}, result: false},
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
