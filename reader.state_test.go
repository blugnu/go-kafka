package kafka

import (
	"testing"
)

func TestReaderStateString(t *testing.T) {
	testcases := []struct {
		name string
		v    readerState
		s    string
	}{
		{name: "rsInitialising", v: rsInitialising, s: "rsInitialising / <not set>"},
		{name: "rsInitialised", v: rsInitialised, s: "rsInitialised"},
		{name: "rsConnected", v: rsConnected, s: "rsConnected"},
		{name: "rsClosed", v: rsClosed, s: "rsClosed"},
		{name: "(invalid)", v: readerState(-1), s: "<undefined>"},
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
