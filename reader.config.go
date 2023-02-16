package kafka

import (
	"strings"

	"github.com/blugnu/kafka/api"
)

func (r *Reader) config() *api.ConfigMap {
	return &api.ConfigMap{
		"group.id":           r.groupId,
		"bootstrap.servers":  strings.Join(BootstrapServers, ","),
		"enable.auto.commit": "false",
		"auto.offset.reset":  "earliest",
	}
}
