package confluent

import (
	"errors"
	"testing"
)

func TestConsumer_Close(t *testing.T) {
	// ARRANGE
	fnerr := errors.New("func error")
	sut := &Consumer{
		funcs: &ConsumerFuncs{
			Close: func() error { return fnerr },
		},
	}

	// ACT
	err := sut.Close()

	// ASSERT
	t.Run("calls funcs.Close", func(t *testing.T) {
		t.Run("returns error", func(t *testing.T) {
			wanted := fnerr
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})
}
