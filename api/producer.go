package api

import "github.com/confluentinc/confluent-kafka-go/kafka"

type ProducerApi interface {
	Close()
	Create(*kafka.ConfigMap) error
	GetEventChannel() chan kafka.Event
	Flush(int) int
	Produce(*kafka.Message, chan kafka.Event) error
}
