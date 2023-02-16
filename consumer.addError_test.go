package kafka

import (
	"errors"
	"testing"
)

func TestConsumer_AddError(t *testing.T) {
	// ARRANGE
	sut := &Consumer{}
	cerr := errors.New("consumer error")

	// ACT
	err := sut.addError(cerr, csStoppedWithError)

	// ASSERT
	t.Run("adds error", func(t *testing.T) {
		wanted := []error{cerr}
		got := sut.errors
		if len(got) == 0 || got[0] != cerr {
			t.Errorf("\nwanted %v\ngot    %v", wanted, got)
		}
	})

	t.Run("returns error added", func(t *testing.T) {
		wanted := cerr
		got := err
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
	t.Run("sets consumers state", func(t *testing.T) {
		wanted := csStoppedWithError
		got := sut.state
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}
