package kafka

import (
	"context"
	"time"

	"github.com/blugnu/kafka/api"
)

func (r *Reader) ReadAll(ctx context.Context, timeout time.Duration) ([]*Message, error) {
	if err := r.connect(ctx); err != nil {
		return nil, err
	}

	topics := []string{}
	for _, t := range r.Topics {
		topic, _ := IdForTopic(t)
		topics = append(topics, topic)
	}

	if err := r.api.Assign(topics); err != nil {
		return nil, ApiError{"Assign", err}
	}
	defer r.api.Assign(nil)

	msgs := []*Message{}
	for {
		msg, err := r.api.ReadMessage(timeout)
		if err == api.ErrTimeout {
			return msgs, nil
		}
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, fromApiMessage(msg))
	}
}
