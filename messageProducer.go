//go:build !async_producer

package kafka

import (
	"context"

	"github.com/blugnu/kafka/api"
)

type MessageProducer interface {
	// Produce is only required/support for asyn message production which is not yet tested
	// in this module.  Current async support is contingent on building with an "async_producer" tag
	//	Produce(context.Context, *Message) error
	MustProduce(context.Context, *Message) (*api.Offset, error)
}
