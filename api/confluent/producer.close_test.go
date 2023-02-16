package confluent

import (
	"testing"
)

func TestProducer_Close(t *testing.T) {
	// ARRANGE
	closeFuncCalled := false

	sut := &Producer{
		funcs: &ProducerFuncs{
			Close: func() { closeFuncCalled = true },
		},
	}

	// ACT
	sut.Close()

	// ASSERT
	t.Run("calls funcs.Close", func(t *testing.T) {
		wanted := true
		got := closeFuncCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}
