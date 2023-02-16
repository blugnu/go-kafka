package kafka

import (
	"time"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/context"
)

// consumerMessage reads a message from the consumer, passes it through the
// EncryptionHandler to be decrypted then to the message handler function
// for the topic from which the message was read.
//
// If a message is read successfully (even if decryption or the handler returns
// an error) the function returns a pointer to the Offset information for the
// message (and any error).
//
// If the consumer times-out waiting for a new message, no message is read and
// a nil Offset is returned, along with an ErrTimeout error.
func (c *Consumer) consumeMessage(ctx context.Context) (*api.Offset, error) {
	apimsg, err := c.api.ReadMessage(100 * time.Millisecond)
	if err == api.ErrTimeout {
		return nil, ErrTimeout
	}
	if err != nil {
		return nil, ApiError{"ReadMessage", err}
	}

	msg := fromApiMessage(apimsg)

	ctx = context.WithMessageReceived(ctx, msg.summary())

	addr := &api.Offset{
		Topic:     msg.Topic,
		Partition: *msg.Partition,
		Offset:    *msg.Offset,
	}
	err = c.Decrypt(msg)
	if err != nil {
		return addr, EncryptionHandlerError{err}
	}

	if err := c.handleMessage(ctx, msg); err != nil {
		return addr, HandlerError{err}
	}

	return addr, nil
}
