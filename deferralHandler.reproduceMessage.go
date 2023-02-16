package kafka

import (
	"context"
	"time"
)

// ReproduceMessage implements a DeferralPolicy that waits for a specified Delay period
// before producing a copy of the deferred message with an adjusted deferral header
// reflecting the time elapsed since the deferred message was read.  If the adjusted
// deferral duration is zero or negative, the message is produced without any deferral
// header; the consumer will then process that message and not defer it further.
//
// A Delay may be specified to introduce a sleep period before the message is reproduced.
// This may be used if it is necessary or desirable to reduce the volume of messages
// produced by the policy.  Any Delay will block the consumer, preventing it from
// processing other messages from other partitions or topics.  Therefore, any Delay should
// be short, typically 10-100ms.  The default is 0, which does not delay or block at all.
//
// This deferral policy produces a potentially large volume of messages but avoids
// blocking other messages in the same topic partition (aside from any configured Delay).
type ReproduceMessage struct {
	Delay time.Duration
}

func (dp *ReproduceMessage) Defer(ctx context.Context, c *Consumer, msg *Message) error {
	time.Sleep(dp.Delay)

	deferred := msg.NewCopy()
	deferred.Topic = msg.Topic

	_, d := msg.isDeferred()
	if *msg.Age < d {
		d = d - *msg.Age
		deferred.DeferFor(d)
	}

	_, err := c.MustProduce(ctx, deferred)
	return err
}
