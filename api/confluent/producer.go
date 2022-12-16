package api

import "github.com/confluentinc/confluent-kafka-go/kafka"

type producer struct {
	*kafka.Producer
}

var _producer = &producer{}

func ConfluentProducerApi() ProducerApi {
	return _producer
}

func (p *producer) Close() {
	p.Producer.Close()
}

func (p *producer) Create(cfg *kafka.ConfigMap) error {
	if cfg == nil {
		p.Producer = &kafka.Producer{}
		return nil
	}

	var err error
	p.Producer, err = kafka.NewProducer(cfg)
	return err
}

func (p *producer) GetEventChannel() chan kafka.Event {
	return p.Events()
}

func (p *producer) Flush(timeoutMs int) int {
	return p.Producer.Flush(timeoutMs)
}

func (p *producer) Produce(msg *kafka.Message, ch chan kafka.Event) error {
	return p.Producer.Produce(msg, ch)
}
