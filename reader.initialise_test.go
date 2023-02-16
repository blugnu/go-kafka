package kafka

import (
	"context"
	"testing"

	"github.com/blugnu/kafka/api/confluent"
	"github.com/blugnu/logger"
)

func TestReader_initialise(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	newreader := func() *Reader {
		return &Reader{
			log:    Log,
			Topics: []string{"topic"},
		}
	}

	t.Run("with no configuration", func(t *testing.T) {
		// ARRANGE
		sut := newreader()

		// ACT
		err := sut.initialise(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT base logger configuration
		t.Run("configures base logger", func(t *testing.T) {
			t.Run("of expected type", func(t *testing.T) {
				wanted := &logger.Base{}
				got := sut.log
				if got == nil {
					t.Errorf("\nwanted %T\ngot    nil", wanted)
				}
			})

			t.Run("with expected context", func(t *testing.T) {
				wanted := ctx
				got := sut.log.Context
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})
		})

		// ASSERT default Api configuration
		t.Run("configures confluent consumer api", func(t *testing.T) {
			wanted := &confluent.Consumer{}
			got := sut.api
			if got == nil {
				t.Errorf("\nwanted %T\ngot    nil", wanted)
			}
		})

		// ASSERT default EncryptionHandler configuration
		t.Run("configures default EncryptionHandler", func(t *testing.T) {
			wanted := Encryption
			got := sut.EncryptionHandler
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("with no default encryption handler", func(t *testing.T) {
		// ARRANGE
		sut := newreader()

		og := Encryption
		defer func() { Encryption = og }()
		Encryption = nil

		// ACT
		err := sut.initialise(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("configures EncryptionHandler", func(t *testing.T) {
			wanted := &NoEncryption{}
			got := sut.EncryptionHandler
			if _, ok := got.(*NoEncryption); !ok {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("with no topics specified", func(t *testing.T) {
		// ARRANGE
		sut := newreader()
		sut.Topics = nil

		// ACT
		err := sut.initialise(ctx)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := ErrNoTopics
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})
}
