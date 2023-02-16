package set

import (
	"fmt"
	"testing"
)

func TestEqual(t *testing.T) {
	testcases := []struct {
		a      []int
		b      []int
		result bool
	}{
		{a: []int{}, b: []int{}, result: true},
		{a: []int{}, b: []int{0}, result: false},
		{a: []int{0}, b: []int{}, result: false},
		{a: []int{0}, b: []int{0}, result: true},
		{a: []int{0}, b: []int{1}, result: false},
		{a: []int{0, 1}, b: []int{0, 1}, result: true},
		{a: []int{0, 1}, b: []int{1, 0}, result: true},
		{a: []int{0, 1, 2, 3}, b: []int{0, 1, 2, 3}, result: true},
		{a: []int{0, 1, 2, 3}, b: []int{3, 2, 1, 0}, result: true},
		{a: []int{0, 1, 2, 3}, b: []int{3, 2, 1}, result: false},
		{a: []int{0, 1, 2}, b: []int{3, 2, 1, 0}, result: false},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%v vs %v", tc.a, tc.b), func(t *testing.T) {
			// ACT
			got := FromSlice(tc.a).Equal(FromSlice(tc.b))

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}
}
