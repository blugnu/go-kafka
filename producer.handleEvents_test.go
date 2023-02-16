package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

type deliveryEventId int

const (
	onMessageDelivered deliveryEventId = iota
	onMessageError
	onProducerError
	onUnexpectedDeliveryEvent
)

type EventRecorder struct {
	Called deliveryEventId
	api.DeliveryEvent
	*Message
	Error  error
	Closed bool
}

func (h *EventRecorder) OnMessageDelivered(msg *Message) {
	h.Called = onMessageDelivered
	h.Message = msg
	h.Error = nil
	h.DeliveryEvent = nil
	h.Closed = false
}

func (h *EventRecorder) OnMessageError(msg *Message, err error, close *bool) {
	h.Called = onMessageDelivered
	h.Message = msg
	h.Error = err
	h.DeliveryEvent = nil
	*close = h.Closed
}

func (h *EventRecorder) OnProducerError(err error, close *bool) {
	h.Called = onMessageDelivered
	h.Message = nil
	h.Error = err
	h.DeliveryEvent = nil
	*close = h.Closed
}

func (h *EventRecorder) OnUnexpectedEvent(ev api.DeliveryEvent, close *bool) {
	h.Called = onMessageDelivered
	h.Message = nil
	h.Error = nil
	h.DeliveryEvent = ev
	*close = h.Closed
}

func TestProducer_HandleEvents(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	// ARRANGE
	msgerr := errors.New("message error")
	proderr := confluent.NewError(confluent.ErrAllBrokersDown, "producer error", true)
	otherev := MockEvent("other event")

	topic := "topic"
	partition := int32(1)
	offset := int64(1)
	deliveredMsg := &confluent.Message{
		TopicPartition: confluent.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    confluent.Offset(offset),
		},
	}
	deliveredMsg.Headers = append([]confluent.Header{}, confluent.Header{Key: "key", Value: []byte("value")})

	failedMsg := &confluent.Message{
		TopicPartition: confluent.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    confluent.Offset(offset),
			Error:     msgerr,
		},
	}
	failedMsg.Headers = append([]confluent.Header{}, confluent.Header{Key: "key", Value: []byte("value")})

	eventMsg := &Message{
		Topic:      topic,
		Partition:  &partition,
		Offset:     &offset,
		Headers:    Headers{"key": "value"},
		RawHeaders: RawHeaders{"key": []byte("value")},
	}

	papi, mockapi := mock.ProducerApi()
	sut := &Producer{
		api: papi,
	}

	testcases := []struct {
		name     string
		event    api.DeliveryEvent
		expected *EventRecorder
	}{
		{name: "message delivered", event: deliveredMsg, expected: &EventRecorder{Called: onMessageDelivered, Message: eventMsg}},
		{name: "message error (producer not closed)", event: failedMsg, expected: &EventRecorder{Called: onMessageError, Message: eventMsg, Error: msgerr, Closed: false}},
		{name: "message error (producer closed)", event: failedMsg, expected: &EventRecorder{Called: onMessageError, Message: eventMsg, Error: msgerr, Closed: true}},
		{name: "producer error (not closed)", event: proderr, expected: &EventRecorder{Called: onProducerError, Error: proderr, Closed: false}},
		{name: "producer error (closed)", event: proderr, expected: &EventRecorder{Called: onProducerError, Error: proderr, Closed: true}},
		{name: "other event (not closed)", event: otherev, expected: &EventRecorder{Called: onUnexpectedDeliveryEvent, DeliveryEvent: otherev, Closed: false}},
		{name: "other event (closed)", event: otherev, expected: &EventRecorder{Called: onUnexpectedDeliveryEvent, DeliveryEvent: otherev, Closed: true}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			defer mockapi.Reset()

			mockapi.SendDeliveryEvent(tc.event, !tc.expected.Closed)

			recorded := &EventRecorder{Closed: tc.expected.Closed}

			// ACT
			sut.handleEvents(ctx, recorded)

			// ASSERT
			t.Run("calls event", func(t *testing.T) {
				wanted := tc.expected.DeliveryEvent
				got := recorded.DeliveryEvent
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})

			t.Run("with message", func(t *testing.T) {
				wanted := tc.expected.Message
				got := recorded.Message
				if !got.Equal(wanted) {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})

			t.Run("with error", func(t *testing.T) {
				wanted := tc.expected.Error
				got := recorded.Error
				if wanted != got {
					t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
				}
			})

			t.Run("with event", func(t *testing.T) {
				wanted := tc.expected.DeliveryEvent
				got := recorded.DeliveryEvent
				if wanted != got {
					t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
				}
			})
		})
	}
}
