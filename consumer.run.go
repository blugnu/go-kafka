package kafka

import (
	"github.com/blugnu/kafka/context"
)

// Run starts the consumer, consuming messages from all topics for which
// a message handler is configured and any retry topics.
//
// If an error occurs starting the consumer this will be returned.
func (c *Consumer) Run(ctx context.Context) error {
	if c.state != csInitialising {
		return InvalidStateError{operation: "Run", state: c.state.String()}
	}

	ctx = context.WithGroupId(ctx, c.GroupId)

	if err := c.initialise(ctx); err != nil {
		return c.addError(err, csInitialiseFailed)
	}

	if err := c.connect(ctx); err != nil {
		return c.addError(err, csConnectFailed)
	}

	if err := c.subscribe(ctx); err != nil {
		return c.addError(err, csSubscribeFailed)
	}

	go c.consumeMessages(ctx)

	return nil
}
