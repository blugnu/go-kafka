package xfmt

import (
	"fmt"
	"testing"
	"time"
)

func TestDuration(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		d      time.Duration
		msa    int
		result string
	}{
		{d: (35 * 24 * time.Hour) + (15 * time.Minute) + (1 * time.Millisecond), result: "1m 5d 0h 15m 0.001s"},
		{d: (31 * 24 * time.Hour) + (15 * time.Minute) + (1 * time.Millisecond), result: "1m 1d 0h 15m 0.001s"},
		{d: (29 * 24 * time.Hour) + (15 * time.Minute) + (1 * time.Millisecond), result: "4wk 1d 0h 15m 0.001s"},
		{d: (21 * 24 * time.Hour) + (15 * time.Minute) + (1 * time.Millisecond), result: "3wk 0h 15m 0.001s"},
		{d: (16 * 24 * time.Hour) + (23 * time.Minute) + (16 * time.Second) + (980 * time.Millisecond), result: "2wk 2d 0h 23m 16.980s"},
		{d: (4 * 24 * time.Hour) + (23 * time.Minute) + (16 * time.Second) + (980 * time.Millisecond), result: "4d 0h 23m 16.980s"},
		{d: (4 * 24 * time.Hour) + (12 * time.Hour) + (24 * time.Minute), result: "4d 12h 24m 0.000s"},
		{d: (4 * time.Hour) + (59 * time.Minute), result: "4h 59m 0.000s"},
		{d: (4 * time.Hour) + (59 * time.Minute), msa: 5, result: "4h 59m 0.00000s"},
	}

	// ACT
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			if tc.msa != 0 {
				MillisecondAccuracy = tc.msa
			}

			wanted := tc.result
			got := Duration(tc.d)
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}
}
