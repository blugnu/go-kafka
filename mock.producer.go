package kafka

import (
	"context"

	"github.com/blugnu/kafka/api"
)

type MockProducer struct {
	AllMessages []*Message
	Messages    map[string][]*Message
	error
}

func (p *MockProducer) record(msg *Message) *api.Offset {
	if p.Messages == nil {
		p.Messages = map[string][]*Message{}
	}

	t, ok := p.Messages[msg.Topic]
	if !ok {
		t = []*Message{}
	}
	n := int64(len(t))
	pt := int32(1)

	cpy := msg.NewCopy()
	cpy.Topic = msg.Topic
	cpy.Partition = &pt
	cpy.Offset = &n

	for k, v := range msg.RawHeaders {
		cpy.SetRawHeader(k, v)
	}
	p.Messages[msg.Topic] = append(t, cpy)
	p.AllMessages = append(p.AllMessages, cpy)

	return &api.Offset{Topic: cpy.Topic, Partition: 1, Offset: n}
}

func (p *MockProducer) MustProduce(ctx context.Context, msg *Message) (*api.Offset, error) {
	if p.error != nil {
		return nil, p.error
	}
	return p.record(msg), nil
}

func (p *MockProducer) Reset() {
	p.AllMessages = []*Message{}
	p.Messages = map[string][]*Message{}
	p.error = nil
}
