package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/context"
	"github.com/blugnu/logger"
)

type ProducerMode int

const (
	ProducerModeNotSet ProducerMode = iota
	IdempotentDelivery
	AtLeastOnceDelivery
)

type EventChannel chan kafka.Event

type Producer struct {
	EncryptionHandler
	EventChannel
	Api   api.Producer
	Log   *logger.Base
	Mode  ProducerMode
	api   api.Producer
	log   *logger.Base
	state producerState
}

type ProducerEventHandler interface {
	OnMessageDelivered(*Message)
	OnMessageError(*Message, error, *bool)
	OnProducerError(error, *bool)
	OnUnexpectedEvent(api.DeliveryEvent, *bool)
}

func (p *Producer) getLog() *logger.Log {
	return p.log.Log()
}

func (p *Producer) Close() {
	p.api.Close()
}

// MustProduce uses the default producer to produce a specified message.
func MustProduce(ctx context.Context, msg *Message) (*Offset, error) {
	if DefaultProducer == nil {
		return nil, ErrNoDefaultProducer
	}
	return DefaultProducer.MustProduce(ctx, msg)
}
