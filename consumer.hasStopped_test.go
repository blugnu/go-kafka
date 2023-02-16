package kafka

import "testing"

func TestConsumer_HasStopped(t *testing.T) {
	testcases := []struct {
		state  consumerState
		result bool
	}{
		{state: csInitialising, result: false},
		{state: csRunning, result: false},
		{state: csStopped, result: true},
		{state: csClosed, result: true},
		{state: csInitialiseFailed, result: true},
		{state: csConnectFailed, result: true},
		{state: csSubscribeFailed, result: true},
		{state: csStoppedWithError, result: true},
		{state: csCloseFailed, result: true},
	}
	for _, tc := range testcases {
		t.Run(tc.state.String(), func(t *testing.T) {
			sut := &Consumer{state: tc.state}
			// ACT
			got := sut.HasStopped()

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}
}
