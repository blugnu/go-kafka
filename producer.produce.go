//go:build async_producer

package kafka

import "context"

func (p *Producer) HandleEvents(ctx context.Context, handler ProducerEventHandler) {
	go p.handleEvents(ctx, handler)
}

// Produce produces a message.  Delivery events are received over the producer.EventChannel
// and must be handled by establishing an event handler goroutine by a previous call to
// HandleEvents().
func (p *Producer) Produce(ctx context.Context, msg *Message) error {
	err := p.Encrypt(msg)
	if err != nil {
		return err
	}
	return p.api.Produce(msg.asApiMessage(), nil)
}
