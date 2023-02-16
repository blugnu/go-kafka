package confluent

import (
	"errors"
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestNewAdminClientFromConsumer(t *testing.T) {
	// ARRANGE
	ogf := newAdminClientFromConsumer
	defer func() { newAdminClientFromConsumer = ogf }()

	t.Run("when successful", func(t *testing.T) {
		cac := &confluent.AdminClient{}
		newAdminClientFromConsumer = func(*confluent.Consumer) (*confluent.AdminClient, error) {
			return cac, nil
		}

		// ACT
		ac, err := NewAdminClientFromConsumer(&confluent.Consumer{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		t.Run("returns admin client from confluent", func(t *testing.T) {
			// ASSERT
			wanted := cac
			got := ac.(*adminClient).AdminClient
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("when confluent api returns error", func(t *testing.T) {
		cferr := errors.New("confluent error")
		newAdminClientFromConsumer = func(*confluent.Consumer) (*confluent.AdminClient, error) {
			return nil, cferr
		}

		// ACT
		ac, err := NewAdminClientFromConsumer(&confluent.Consumer{})

		t.Run("returns nil client wrapper", func(t *testing.T) {
			// ASSERT
			wanted := (AdminClient)(nil)
			got := ac
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("returns confluent error", func(t *testing.T) {
			// ASSERT
			wanted := cferr
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})
}
