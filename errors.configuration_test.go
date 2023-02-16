package kafka

import (
	"errors"
	"testing"
)

func TestConfigurationError(t *testing.T) {
	// ARRANGE
	cfgerr := errors.New("configuration error")

	t.Run("error string", func(t *testing.T) {
		testcases := []struct {
			name string
			error
			result string
		}{
			{name: "general error", error: ConfigurationError{error: cfgerr}, result: "configuration error: " + cfgerr.Error()},
			{name: "topic error", error: ConfigurationError{Topic: "topic", error: cfgerr}, result: "topic configuration error: " + cfgerr.Error()},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				s := tc.error.Error()

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
		topicerr := ConfigurationError{"topic", cfgerr}
		generr := ConfigurationError{error: cfgerr}

		testcases := []struct {
			name string
			error
			other  error
			result bool
		}{
			{name: "topic err: same error, same topic", error: topicerr, other: ConfigurationError{"topic", cfgerr}, result: true},
			{name: "topic err: same error, different topic", error: topicerr, other: ConfigurationError{"other", cfgerr}, result: false},
			{name: "topic err: different error, same topic", error: topicerr, other: ConfigurationError{"topic", errors.New("other error")}, result: false},
			{name: "topic err: different error, different topic", error: topicerr, other: ConfigurationError{"other", errors.New("other error")}, result: false},
			{name: "general error: same error, no topic", error: generr, other: ConfigurationError{error: cfgerr}, result: true},
			{name: "general error: same error, topic specific", error: generr, other: ConfigurationError{"topic", cfgerr}, result: false},
			{name: "general error: different error, no topic", error: generr, other: ConfigurationError{error: errors.New("other error")}, result: false},
			{name: "general error: different error, topic specific", error: generr, other: ConfigurationError{"topic", errors.New("other error")}, result: false},
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
