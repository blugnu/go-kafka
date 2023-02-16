package confluent

import (
	"github.com/blugnu/kafka/api"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func (c *Consumer) Subscribe(ta []string, onRebalance api.RebalanceEventHandler) error {
	if len(ta) == 0 {
		return api.ErrNoTopics
	}

	rbc := (confluent.RebalanceCb)(nil)

	if onRebalance != nil {
		rbc = func(_ *confluent.Consumer, ev confluent.Event) error {
			event, offsets, err := parseRebalanceEvent(ev)
			if err != nil {
				return err
			}
			return onRebalance(event, offsets)
		}
	}

	return c.funcs.SubscribeTopics(ta, rbc)
}
