package confluent

import (
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestProducer_EventChannel(t *testing.T) {
	// ARRANGE
	funcCalled := false

	sut := &Producer{
		funcs: &ProducerFuncs{
			EventChannel: func() chan confluent.Event { funcCalled = true; return nil },
		},
	}

	// ACT
	sut.EventChannel()

	// ASSERT
	t.Run("calls funcs.EventChannel", func(t *testing.T) {
		wanted := true
		got := funcCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}
