package kafka

import (
	"strings"

	"github.com/blugnu/kafka/api"
)

func (p *Producer) config() *api.ConfigMap {
	cfg := &api.ConfigMap{
		"bootstrap.servers":  strings.Join(BootstrapServers, ","),
		"enable.idempotence": p.Mode == IdempotentDelivery || (p.Mode == ProducerModeNotSet && Delivery == IdempotentDelivery),
		"batch.size":         1,
		"linger.ms":          0,
	}

	return cfg
}
