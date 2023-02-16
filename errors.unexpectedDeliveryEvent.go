package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type UnexpectedDeliveryEvent struct {
	event kafka.Event
}

func (e UnexpectedDeliveryEvent) Error() string {
	return fmt.Sprintf("unexpected delivery event (%[1]T): %[1]s", e.event)
}
