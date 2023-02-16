package kafka

import (
	"context"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

// HandleEvents starts a goroutine that processes events arriving on the producer
// Event channel.  Events are dispatched to the appropriate method of the
// supplied event handler interface.
func (p *Producer) handleEvents(ctx context.Context, handler ProducerEventHandler) {
	defer p.Close()

	close := false
	events := p.api.EventChannel()

	for e := range events {
		switch ev := e.(type) {
		case *confluent.Message:
			msg := &Message{
				Topic:      *ev.TopicPartition.Topic,
				Partition:  &ev.TopicPartition.Partition,
				Offset:     (*int64)(&ev.TopicPartition.Offset),
				Key:        ev.Key,
				Value:      ev.Value,
				Headers:    Headers{},
				RawHeaders: RawHeaders{},
			}
			for _, h := range ev.Headers {
				msg.Headers[h.Key] = string(h.Value)
				msg.RawHeaders[h.Key] = h.Value
			}

			// The message delivery report, indicating success or
			// permanent failure after retries have been exhausted.
			if err := ev.TopicPartition.Error; err != nil {
				handler.OnMessageError(msg, ev.TopicPartition.Error, &close)
			} else {
				handler.OnMessageDelivered(msg)
			}

		case confluent.Error:
			// Generic client instance-level errors, such as
			// broker connection failures, authentication issues, etc.
			//
			// These errors should generally be considered informational
			// as the underlying client will automatically try to
			// recover from any errors encountered, the application
			// does not need to take action on them.
			handler.OnProducerError(ev, &close)

		default:
			handler.OnUnexpectedEvent(ev, &close)
		}

		// If a ProducerError or UnexpectedEvent handler signals that the
		// producer should close then return from this function, which
		// (which closes the producer via the deferred call to p.close())
		if close {
			return
		}
	}
}
