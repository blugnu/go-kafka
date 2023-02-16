package api

import (
	"time"
)

type Consumer interface {
	Assign([]string) error
	Close() error
	Create(*ConfigMap) error
	Commit(*Offset, CommitIntent) error
	ReadMessage(time.Duration) (*Message, error)
	Seek(*Offset, time.Duration) error
	Subscribe([]string, RebalanceEventHandler) error
}

type Producer interface {
	Close()
	Create(*ConfigMap) error
	EventChannel() chan DeliveryEvent
	Flush(int) int
	Produce(*Message, chan DeliveryEvent) error
}
