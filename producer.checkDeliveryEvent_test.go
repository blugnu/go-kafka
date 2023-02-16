package kafka

import (
	"errors"
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestCheckDeliveryEvent(t *testing.T) {
	// ARRANGE
	sut := &Producer{}

	t.Run("is a message", func(t *testing.T) {
		// ARRANGE
		topic := "topic"
		partition := int32(1)
		offset := int64(1)
		ev := &confluent.Message{
			TopicPartition: confluent.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Offset:    confluent.Offset(offset),
			},
		}

		// ACT
		msg, err := sut.checkDeliveryEvent(ev)

		// ASSERT
		t.Run("message", func(t *testing.T) {
			wanted := &Message{
				Topic:     topic,
				Partition: &partition,
				Offset:    &offset,
			}
			got := msg
			if !got.Equal(wanted) {
				t.Errorf("wanted %v, got %v", *wanted, *got)
			}
		})

		t.Run("error", func(t *testing.T) {
			wanted := (error)(nil)
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("is a partition error", func(t *testing.T) {
		// ARRANGE
		topic := "topic"
		partition := int32(1)
		offset := int64(1)
		pterr := errors.New("partition error")
		ev := &confluent.Message{
			TopicPartition: confluent.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Offset:    confluent.Offset(offset),
				Error:     pterr,
			},
		}

		// ACT
		msg, err := sut.checkDeliveryEvent(ev)

		// ASSERT
		t.Run("message", func(t *testing.T) {
			wanted := &Message{
				Topic:     topic,
				Partition: &partition,
				Offset:    &offset,
			}
			got := msg
			if !got.Equal(wanted) {
				t.Errorf("wanted %v, got %v", *wanted, *got)
			}
		})

		t.Run("error", func(t *testing.T) {
			wanted := DeliveryFailure{pterr}
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("is an error", func(t *testing.T) {
		// ARRANGE
		ev := confluent.NewError(confluent.ErrAllBrokersDown, "brokers are down", true)

		// ACT
		msg, err := sut.checkDeliveryEvent(ev)

		// ASSERT
		t.Run("message", func(t *testing.T) {
			wanted := (*Message)(nil)
			got := msg
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("error", func(t *testing.T) {
			wanted := DeliveryFailure{ev}
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})

	t.Run("is some other event", func(t *testing.T) {
		// ARRANGE
		ev := MockEvent("some event")

		// ACT
		msg, err := sut.checkDeliveryEvent(ev)

		// ASSERT
		t.Run("message", func(t *testing.T) {
			wanted := (*Message)(nil)
			got := msg
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("error", func(t *testing.T) {
			wanted := DeliveryFailure{UnexpectedDeliveryEvent{ev}}
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})
	})
}
