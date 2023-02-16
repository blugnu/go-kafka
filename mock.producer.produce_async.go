//go:build async_producer

package kafka

import (
	"context"
)

func (p *MockProducer) Produce(ctx context.Context, msg *Message) error {
	if p.error != nil {
		return p.error
	}
	p.record(msg)
	return nil
}
