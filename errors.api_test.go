package kafka

import (
	"errors"
	"fmt"
	"testing"
)

func TestApiError(t *testing.T) {
	// ARRANGE
	apierr := errors.New("api error")

	t.Run("error string", func(t *testing.T) {
		testcases := []struct {
			operation string
			result    string
		}{
			{operation: "Op", result: "api.Op() error: api error"},
			{operation: "", result: "api error: api error"},
		}
		for _, tc := range testcases {
			t.Run(fmt.Sprintf("operation: %s", tc.operation), func(t *testing.T) {
				// ACT
				s := ApiError{tc.operation, apierr}.Error()

				// ASSERT
				wanted := tc.result
				got := s
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})
		}
	})

	t.Run("is", func(t *testing.T) {
		operr := ApiError{"Run", apierr}
		generr := ApiError{error: apierr}

		testcases := []struct {
			name string
			error
			other  error
			result bool
		}{
			{name: "operation err: same error, same operation", error: operr, other: ApiError{"Run", apierr}, result: true},
			{name: "operation err: same error, different operation", error: operr, other: ApiError{"Other", apierr}, result: false},
			{name: "operation err: different error, same operation", error: operr, other: ApiError{"Run", errors.New("other error")}, result: false},
			{name: "operation err: different error, different operation", error: operr, other: ApiError{"Other", errors.New("other error")}, result: false},
			{name: "general error: same error, no operation", error: generr, other: ApiError{error: apierr}, result: true},
			{name: "general error: same error, operation specific", error: generr, other: ApiError{"Run", apierr}, result: false},
			{name: "general error: different error, no operation", error: generr, other: ApiError{error: errors.New("other error")}, result: false},
			{name: "general error: different error, operation specific", error: generr, other: ApiError{"Run", errors.New("other error")}, result: false},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				wanted := tc.result
				got := errors.Is(tc.error, tc.other)

				// ASSERT
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})
		}
	})
}
