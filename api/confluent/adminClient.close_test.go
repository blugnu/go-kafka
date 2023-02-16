package confluent

import (
	"testing"
)

func TestAdminClient_Close(t *testing.T) {
	// ARRANGE
	fncalled := false
	sut := &adminClient{
		funcs: &adminClientFuncs{
			Close: func() { fncalled = true },
		},
	}

	// ACT
	sut.Close()

	// ASSERT
	t.Run("calls funcs.Close", func(t *testing.T) {
		wanted := true
		got := fncalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}
