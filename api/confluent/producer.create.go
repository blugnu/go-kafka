package confluent

import (
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/blugnu/kafka/api"
)

var newProducer = confluent.NewProducer

func (p *Producer) Create(cfg *api.ConfigMap) (err error) {
	cm := &confluent.ConfigMap{}
	for k, v := range *cfg {
		cm.SetKey(k, v)
	}

	if p.Producer, err = newProducer(cm); err != nil {
		return
	}

	p.funcs = &ProducerFuncs{
		Close:        p.Producer.Close,
		EventChannel: p.Events,
		Flush:        p.Flush,
		Produce:      p.Producer.Produce,
	}

	return
}
