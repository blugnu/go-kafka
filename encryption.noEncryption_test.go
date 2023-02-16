package kafka

import "testing"

func TestNoEncryption(t *testing.T) {
	// ARRANGE
	msg := &Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	sut := &NoEncryption{}

	t.Run("stringer", func(t *testing.T) {
		// ACT
		s := sut.String()

		// ASSERT
		wanted := "NoEncryption"
		got := s
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("encrypt", func(t *testing.T) {
		// ARRANGE
		emsg := msg.NewCopy()

		// ACT
		err := sut.Encrypt(emsg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("resulting message", func(t *testing.T) {
			wanted := msg
			got := emsg
			if !wanted.Equal(got) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

	})

	t.Run("decrypt", func(t *testing.T) {
		// ARRANGE
		dmsg := msg.NewCopy()

		// ACT
		err := sut.Decrypt(dmsg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("resulting message", func(t *testing.T) {
			wanted := msg
			got := dmsg
			if !wanted.Equal(got) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

	})
}
