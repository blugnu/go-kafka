package confluent

import (
	"github.com/blugnu/kafka/api"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func parseRebalanceEvent(ev confluent.Event) (api.RebalanceEvent, []api.Offset, error) {
	var event api.RebalanceEvent
	var tps []confluent.TopicPartition
	var offsets []api.Offset

	switch e := ev.(type) {
	case confluent.AssignedPartitions:
		event = api.AssignedPartitions
		tps = e.Partitions

	case confluent.RevokedPartitions:
		event = api.RevokedPartitions
		tps = e.Partitions

	default:
		return event, []api.Offset{}, api.UnexpectedRebalanceEventError{Event: ev.String()}
	}

	for _, tp := range tps {
		offsets = append(offsets, api.Offset{
			Topic:     *tp.Topic,
			Partition: tp.Partition,
			Offset:    int64(tp.Offset),
		})
	}

	return event, offsets, nil
}
