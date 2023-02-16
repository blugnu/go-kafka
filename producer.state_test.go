package kafka

import (
	"testing"
)

func TestProducerState(t *testing.T) {
	testcases := []struct {
		name string
		v    producerState
		s    string
	}{
		{name: "psInitialising", v: psInitialising, s: "psInitialising / <not set>"},
		{name: "psConnected", v: psConnected, s: "psConnected"},
		{name: "psClosed", v: psClosed, s: "psClosed"},
		{name: "(invalid)", v: producerState(-1), s: "<undefined>"},
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
