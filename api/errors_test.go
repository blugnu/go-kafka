package api

import "testing"

func TestError_Error(t *testing.T) {
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

func TestUnexpectedRebalanceEventError_Error(t *testing.T) {
	// ARRANGE
	err := UnexpectedRebalanceEventError{"some event"}

	// ACT
	got := err.Error()

	// ASSERT
	wanted := "unexpected rebalance event: some event"
	if wanted != got {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}
