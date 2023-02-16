package kafka

import (
	"github.com/blugnu/go-errorcontext"
	"github.com/blugnu/kafka/context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// MustProduce produces a message and does not return until the delivery event for that
// message is received.  The produced message is returned if successful, otherwise an
// error is returned.
func (p *Producer) MustProduce(ctx context.Context, msg *Message) (*Offset, error) {
	if p.state != psConnected {
		return nil, ErrNotConnected
	}

	if msg.Topic == "" {
		return nil, ErrNoTopicId
	}

	ctx = context.WithMessageProduced(ctx, msg.summary())
	log := p.log.LogWithContext(ctx)

	log.Trace("encrypting message")
	err := p.Encrypt(msg)
	if err != nil {
		return nil, errorcontext.Wrap(ctx, EncryptionHandlerError{err}, "encrypt message")
	}

	dc := make(chan kafka.Event)
	defer close(dc)

	log.Trace("producing message")
	if err := p.api.Produce(msg.asApiMessage(), dc); err != nil {
		return nil, errorcontext.Wrap(ctx, ApiError{"Produce", err}, "confluent producer")
	}

	log.Trace("awaiting delivery event")
	event := <-dc

	log.Trace("checking delivery event")
	msg, err = p.checkDeliveryEvent(event)
	if err != nil {
		return nil, errorcontext.Wrap(ctx, err, "delivery failed")
	}

	log.Info("message produced")
	return &Offset{Topic: msg.Topic, Partition: *msg.Partition, Offset: *msg.Offset}, nil
}
