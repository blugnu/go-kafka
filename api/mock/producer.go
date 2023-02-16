package mock

import (
	"github.com/blugnu/kafka/api"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

type producerFuncs struct {
	Close        func()
	Create       func(*api.ConfigMap) error
	EventChannel func() chan api.DeliveryEvent
	Flush        func(int) int
	Produce      func(*api.Message, chan confluent.Event) error
}

type mockProducer struct {
	CloseWasCalled             bool
	CreateWasCalled            bool
	FlushWasCalled             bool
	ProduceWasCalled           bool
	ResetWasCalled             bool
	SendDeliveryEventWasCalled bool
	FlushTimeoutMs             int
	Config                     *api.ConfigMap
	DeliveryEvents             chan api.DeliveryEvent
	offsets                    map[string]confluent.Offset
	Funcs                      *producerFuncs
}

func ProducerApi() (api.Producer, *mockProducer) {
	p := &mockProducer{}
	p.Reset()
	return p, p
}

func (p *mockProducer) Close() {
	p.CloseWasCalled = true
	p.Funcs.Close()
}

func (p *mockProducer) Create(cfg *api.ConfigMap) error {
	p.Config = cfg
	p.CreateWasCalled = true
	return p.Funcs.Create(cfg)
}

func (p *mockProducer) EventChannel() chan api.DeliveryEvent {
	return p.Funcs.EventChannel()
}

func (p *mockProducer) Flush(timeoutMs int) int {
	p.FlushWasCalled = true
	p.FlushTimeoutMs = timeoutMs
	return p.Funcs.Flush(timeoutMs)
}

func (p *mockProducer) Produce(msg *api.Message, ch chan api.DeliveryEvent) error {
	p.ProduceWasCalled = true
	return p.Funcs.Produce(msg, ch)
}

func (p *mockProducer) Reset() {
	p.Config = nil
	p.ResetWasCalled = true
	p.CloseWasCalled = false
	p.CreateWasCalled = false
	p.FlushWasCalled = false
	p.ProduceWasCalled = false
	p.SendDeliveryEventWasCalled = false

	p.FlushTimeoutMs = 0

	events := make(chan api.DeliveryEvent, 2)
	offsets := map[string]confluent.Offset{}

	p.DeliveryEvents = events
	p.offsets = offsets
	p.Funcs = &producerFuncs{
		Close:        func() { close(events) },
		Create:       func(*api.ConfigMap) error { return nil },
		EventChannel: func() chan api.DeliveryEvent { return events },
		Flush:        func(int) int { return 0 },
		Produce: func(msg *api.Message, dc chan confluent.Event) error {
			offset := offsets[msg.Topic]
			offset++

			if dc != nil {
				go func() {
					kmsg := &confluent.Message{
						TopicPartition: confluent.TopicPartition{
							Topic:     &msg.Topic,
							Partition: 1,
							Offset:    offset,
						},
						Key:   msg.Key,
						Value: msg.Value,
					}
					dc <- kmsg
				}()
			}
			return nil
		},
	}
}

func (p *mockProducer) SendDeliveryEvent(event api.DeliveryEvent, closeChannel bool) {
	p.SendDeliveryEventWasCalled = true
	p.DeliveryEvents <- event
	if closeChannel {
		p.Funcs.Close = func() {}
		close(p.DeliveryEvents)
	}
}
