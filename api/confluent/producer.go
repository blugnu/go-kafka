package confluent

import (
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

type ProducerFuncs struct {
	Close        func()
	EventChannel func() chan confluent.Event
	Flush        func(int) int
	Produce      func(*confluent.Message, chan confluent.Event) error
}

type Producer struct {
	*confluent.Producer
	funcs *ProducerFuncs
}
