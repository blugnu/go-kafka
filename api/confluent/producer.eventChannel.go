package confluent

import confluent "github.com/confluentinc/confluent-kafka-go/kafka"

func (p *Producer) EventChannel() chan confluent.Event {
	return p.funcs.EventChannel()
}
