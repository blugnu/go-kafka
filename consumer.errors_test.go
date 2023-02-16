package kafka

import (
	"errors"
	"testing"
)

func TestConsumer_Errors(t *testing.T) {
	// ARRANGE
	consumererr := errors.New("consumer error")
	sut := &Consumer{
		errors: []error{consumererr},
	}

	// ACT
	errs := sut.Errors()

	// ASSERT
	t.Run("returns expected number of errors", func(t *testing.T) {
		wanted := 1
		got := len(errs)
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns expected errors", func(t *testing.T) {
		wanted := consumererr
		got := errs[0]
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}
