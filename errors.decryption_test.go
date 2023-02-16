package kafka

import (
	"errors"
	"testing"
)

func TestDecryptionError(t *testing.T) {
	// ARRANGE
	dcerr := errors.New("decryption error")
	sut := EncryptionHandlerError{dcerr}

	t.Run("error string", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "encryption handler error: decryption error"
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
			{"same error", EncryptionHandlerError{dcerr}, true},
			{"different error", EncryptionHandlerError{errors.New("other error")}, false},
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
