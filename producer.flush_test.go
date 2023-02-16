package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/blugnu/kafka/api/mock"
)

func TestProducer_Flush(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	papi, mockp := mock.ProducerApi()
	sut := &Producer{
		api: papi,
		log: Log,
	}

	testcases := []struct {
		name      string
		timeout   time.Duration
		timeoutMs int
	}{
		{name: "with timeout 100ms", timeout: 100 * time.Millisecond, timeoutMs: 100},
		{name: "with timeout 500ns", timeout: 500 * time.Nanosecond, timeoutMs: 0},
		{name: "with timeout 0ns", timeout: 0, timeoutMs: 0},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			defer mockp.Reset()

			// ACT
			sut.Flush(tc.timeout)

			// ASSERT
			wanted := tc.timeoutMs
			got := mockp.FlushTimeoutMs
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}
}
