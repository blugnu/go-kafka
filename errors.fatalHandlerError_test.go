package kafka

import (
	"errors"
	"testing"
)

func TestFatalHandlerError(t *testing.T) {
	// ARRANGE
	herr := errors.New("handler error")
	sut := FatalHandlerError{"test", herr}

	t.Run("error string", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "fatal error: test: handler error"
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
			{name: "same reason, same error", error: FatalHandlerError{"test", herr}, result: true},
			{name: "same reason, different error", error: FatalHandlerError{"test", errors.New("other error")}, result: false},
			{name: "different reason, same error", error: FatalHandlerError{"other reason", herr}, result: false},
			{name: "different reason, different error", error: FatalHandlerError{"other reason", errors.New("other error")}, result: false},
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
