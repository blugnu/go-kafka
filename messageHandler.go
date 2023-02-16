package kafka

import (
	"context"
)

type MessageHandler struct {
	Func            func(context.Context, *Message) error
	RetryTopic      string
	DeadLetterTopic string
	DeferralHandler
	RetryPolicy
}

// shouldDefer determines whether the handler should defer processing of the
// specified message.  This is determined by the presence (or not) of a
// `kafka:deferred` header with a `time.Duration` value.
//
// If there is no `kafka:deferred` header or if the message `Age` is greater
// than or equal to the `kafka:deferred` duration, then the message should
// be processed immediately (returns false).
//
// If the message `Age` is less than the `kafka:deferred` duration then the
// message should NOT be processed (returns true).
func (h *MessageHandler) shouldDefer(msg *Message) bool {
	if isDeferred, delay := msg.isDeferred(); isDeferred {
		return *msg.Age < delay
	}
	return false
}
