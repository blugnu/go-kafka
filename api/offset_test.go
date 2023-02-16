package api

import "testing"

func Test_Offset(t *testing.T) {
	testcases := []struct {
		offset   Offset
		expected string
	}{
		{expected: "topic,1:1", offset: Offset{"topic", 1, 1}},
		{expected: "topic.a,2:42", offset: Offset{Topic: "topic.a", Partition: 2, Offset: 42}},
	}
	for _, tc := range testcases {
		t.Run(tc.expected, func(t *testing.T) {
			// ACT
			got := tc.offset.String()

			// ASSERT
			wanted := tc.expected
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}
}
