package kafka

import (
	"fmt"

	"github.com/blugnu/kafka/context"
)

// handleMessage identifies the handler for the specified message and then
// determines whether the message should be handled or deferred.
//
// If the message is to be deferred, the message is passed to the handlers
// configured DeferralHandler.
//
// If the message is NOT to be deferred, then it is passed to the configured
// handler function.  If this returns nil then the message is handled and
// no further processing is required.
func (c *Consumer) handleMessage(ctx context.Context, msg *Message) error {
	log := c.log.LogWithContext(ctx)

	handler := c.handlers[msg.Topic]

	if handler.shouldDefer(msg) {
		log.Trace("deferring message")
		return handler.Defer(ctx, c, msg)
	}

	log.Trace("calling message handler")

	err := handler.Func(ctx, msg)
	if err == nil {
		return nil
	}

	// The handler returned an error...

	// If it has a RetryPolicy...
	if handler.RetryPolicy != nil {
		ctx := context.WithRetryPolicy(ctx, fmt.Sprintf("%T", handler.RetryPolicy))
		log := log.WithContext(ctx)

		log.Trace("applying retry policy")

		if handler.ShouldRetry(msg) {
			retry := msg.NewCopy()
			if retry.Topic = handler.RetryTopic; retry.Topic == "" {
				retry.Topic = msg.Topic
			}

			_, attempt, num := msg.IsRetryAttempt()
			retry.setRetryHeaders(attempt, num+1)

			delay := handler.RetryDelay(retry)
			retry.DeferFor(delay)

			log.Tracef("retry: %s", retry)

			if _, err := c.MustProduce(ctx, retry); err != nil {
				return ProducerError{err}
			}
			return nil
		}
	}

	if handler.DeadLetterTopic != "" {
		log.Warn(err.Error())
		log.Tracef("producing dead letter for: %s", msg)
		msg := msg.NewCopy()
		msg.Topic = handler.DeadLetterTopic
		if _, err := c.MustProduce(ctx, msg); err != nil {
			return ProducerError{err}
		}
		return nil
	}

	// No retry and no deadletter, just return the error
	log.Tracef("no retry, no dead letter for %s, just error %s", msg, err)
	return err
}
