package kafka

import "testing"

func TestError(t *testing.T) {
	// ARRANGE
	sut := Error("foo")

	// ACT
	s := sut.Error()

	// ASSERT
	wanted := "foo"
	got := s
	if wanted != got {
		t.Errorf("wanted %v, got %v", wanted, got)
	}
}
