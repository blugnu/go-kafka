package kafka

import (
	"github.com/blugnu/kafka/api"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// checkDeliveryEvent examines the specified kafka.Event.  If it was a message related
// event then the message is returned; if the message had an error, this also
// is returned (wrapped in a DeliveryFailure{}).
//
// If the event was a kafka.Error (producer client error) then a nil message
// is returned together with the error wrapped in a DeliveryFailure{}.
//
// In all other cases the event is returned as a DeliveryFailure{UnexpectedDeliveryEvent{}}
// (with no offset).
func (p *Producer) checkDeliveryEvent(event api.DeliveryEvent) (*Message, error) {
	switch ev := event.(type) {
	case *kafka.Message:
		msg := fromApiMessage(&api.Message{
			Topic:     *ev.TopicPartition.Topic,
			Partition: &ev.TopicPartition.Partition,
			Offset:    (*int64)(&ev.TopicPartition.Offset),
			Key:       ev.Key,
			Value:     ev.Value,
			Timestamp: &ev.Timestamp,
		})

		if ev.TopicPartition.Error != nil {
			return msg, DeliveryFailure{ev.TopicPartition.Error}
		}

		return msg, nil

	case kafka.Error:
		return nil, DeliveryFailure{ev}
	}

	return nil, DeliveryFailure{UnexpectedDeliveryEvent{event: event}}
}
