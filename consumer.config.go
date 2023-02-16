package kafka

import (
	"strings"

	"github.com/blugnu/kafka/api"
)

// config returns a config map to configure the underlying consumer.
func (c *Consumer) config() *api.ConfigMap {
	return &api.ConfigMap{
		"group.id":           c.GroupId,
		"bootstrap.servers":  strings.Join(BootstrapServers, ","),
		"enable.auto.commit": c.AsyncCommit,
		"auto.offset.reset":  "earliest",
	}
}
