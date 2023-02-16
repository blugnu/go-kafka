package mock

import "testing"

func TestError(t *testing.T) {
	// ARRANGE
	err := Error("error")

	// ACT
	got := err.Error()

	// ASSERT
	wanted := "error"
	if wanted != got {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}
