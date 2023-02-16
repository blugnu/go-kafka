package api

import (
	"fmt"
	"time"
)

type Headers map[string][]byte

type Message struct {
	Topic     string
	Partition *int32
	Offset    *int64
	Key       []byte
	Value     []byte
	Timestamp *time.Time // The Timestamp of the message (non-nil only for messages read by a consumer)
	Headers
}

func (msg *Message) GetOffset() *Offset {
	var ptn int32 = -1
	var off int64 = -1

	if msg.Partition != nil {
		ptn = *msg.Partition
	}
	if msg.Offset != nil {
		off = *msg.Offset
	}

	return &Offset{
		Topic:     msg.Topic,
		Partition: ptn,
		Offset:    off,
	}
}

func (msg *Message) String() string {
	return fmt.Sprintf("%d bytes @ %s", len(msg.Value), msg.GetOffset())
}
