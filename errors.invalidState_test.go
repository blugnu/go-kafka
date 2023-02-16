package kafka

import (
	"errors"
	"testing"
)

func TestInvalidStateError(t *testing.T) {
	// ARRANGE
	sut := InvalidStateError{operation: "Run", state: "csStopped"}

	t.Run("error string", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "operation Run() invalid in state csStopped"
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
			{name: "same operation, same state", error: InvalidStateError{operation: "Run", state: "csStopped"}, result: true},
			{name: "same operation, different state", error: InvalidStateError{operation: "Run", state: "csClosed"}, result: false},
			{name: "different operation, same state", error: InvalidStateError{operation: "Close", state: "csStopped"}, result: false},
			{name: "different operation, different state", error: InvalidStateError{operation: "Close", state: "csClosed"}, result: false},
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
