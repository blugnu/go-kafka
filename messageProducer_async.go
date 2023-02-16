//go:build async_producer

package kafka

import (
	"context"

	"github.com/blugnu/kafka/api"
)

type MessageProducer interface {
	Produce(context.Context, *Message) error
	MustProduce(context.Context, *Message) (*api.Offset, error)
}
