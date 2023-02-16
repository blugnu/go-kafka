package kafka

import (
	"testing"
)

func TestConsumerStateString(t *testing.T) {
	testcases := []struct {
		name string
		v    consumerState
		s    string
	}{
		{name: "csInitialising", v: csInitialising, s: "csInitialising / <not set>"},
		{name: "csRunning", v: csRunning, s: "csRunning"},
		{name: "csStopped", v: csStopped, s: "csStopped"},
		{name: "csInitialiseFailed", v: csInitialiseFailed, s: "csInitialiseFailed"},
		{name: "csConnectFailed", v: csConnectFailed, s: "csConnectFailed"},
		{name: "csSubscribeFailed", v: csSubscribeFailed, s: "csSubscribeFailed"},
		{name: "csCloseFailed", v: csCloseFailed, s: "csCloseFailed"},
		{name: "csStoppedWithError", v: csStoppedWithError, s: "csStoppedWithError"},
		{name: "csClosed", v: csClosed, s: "csClosed"},
		{name: "(invalid)", v: consumerState(-1), s: "<undefined>"},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			s := tc.v.String()

			// ASSERT
			wanted := tc.s
			got := s
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}
}
