package confluent

import (
	"testing"
)

func TestProducer_Flush(t *testing.T) {
	// ARRANGE
	funcCalled := false
	timeoutSpecified := 0

	sut := &Producer{
		funcs: &ProducerFuncs{
			Flush: func(i int) int { funcCalled = true; timeoutSpecified = i; return 0 },
		},
	}

	// ACT
	sut.Flush(42)

	// ASSERT
	t.Run("calls funcs.Flush", func(t *testing.T) {
		wanted := true
		got := funcCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with timeout", func(t *testing.T) {
			wanted := 42
			got := timeoutSpecified
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}
