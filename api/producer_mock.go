package api

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type producerApi struct {
	Close           func()
	Create          func(*kafka.ConfigMap) error
	GetEventChannel func() chan kafka.Event
	Flush           func(int) int
	Produce         func(*kafka.Message, chan kafka.Event) error
}

type mockProducer struct {
	api    producerApi
	events chan kafka.Event
}

type ProducerApiMock interface {
	ProducerApi
	Api() *producerApi
}

func MockProducerApi() (ProducerApi, ProducerApiMock) {
	Events := make(chan kafka.Event)

	result := &mockProducer{
		events: Events,
		api: producerApi{
			Close:           func() { close(Events) },
			Create:          func(*kafka.ConfigMap) error { return nil },
			GetEventChannel: func() chan kafka.Event { return Events },
			Flush:           func(int) int { return 0 },
			Produce:         func(*kafka.Message, chan kafka.Event) error { return nil },
		},
	}
	return result, result
}

func (p *mockProducer) Api() *producerApi {
	return &p.api
}

func (p *mockProducer) Close() {
	p.api.Close()
}

func (p *mockProducer) Create(cfg *kafka.ConfigMap) error {
	return p.api.Create(cfg)
}

func (p *mockProducer) GetEventChannel() chan kafka.Event {
	return p.api.GetEventChannel()
}

func (p *mockProducer) Flush(timeoutMs int) int {
	return p.api.Flush(timeoutMs)
}

func (p *mockProducer) Produce(msg *kafka.Message, ch chan kafka.Event) error {
	return p.api.Produce(msg, ch)
}
