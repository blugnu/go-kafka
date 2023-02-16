package api

import "testing"

func Test_CommitIntent(t *testing.T) {
	testcases := []struct {
		name     string
		value    CommitIntent
		expected string
	}{
		{name: "ReadAgain", value: ReadAgain, expected: "ReadAgain"},
		{name: "ReadNext", value: ReadNext, expected: "ReadNext"},
		{name: "-1", value: CommitIntent(-1), expected: "<undefined>"},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			got := tc.value.String()

			// ASSERT
			wanted := tc.expected
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}
}
