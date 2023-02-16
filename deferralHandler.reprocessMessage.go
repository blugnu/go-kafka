package kafka

import (
	"context"
	"time"

	"github.com/blugnu/kafka/api"
)

// ReprocessMessage implements a DeferralPolicy that forces the consumer to reprocess
// the same message until the time elapsed since the message was originally produced
// equals or exceeds the required deferral period.  This is achieved by Seek'ing the
// consumer offset back to the message so that the consumer will repeatedly re-read
// the same message at the next ReadMessage() request.
//
// A Delay may be specified to avoid the consumer "tight-looping" by sleeping before
// performing the seek and returning.  Any Delay will block the consumer, preventing
// it from processing other messages from other partitions or topics.  Therefore, any
// Delay should be short, typically 10-100ms.  The default is 0, which does not delay
// or block the consumer at all.
//
// This deferral policy avoids producing additional messages but, in addition to blocking
// messages in all partitions processed by the consumer as a result of any Delay, blocks
// completely the processing of other messages in the same topic partition until the
// message has been processed.
type ReprocessMessage struct {
	Delay time.Duration
}

func (dp *ReprocessMessage) Defer(ctx context.Context, c *Consumer, msg *Message) error {
	time.Sleep(dp.Delay)

	log := c.log.LogWithContext(ctx)
	log.Debugf("seeking to %s@%d:%d", msg.Topic, *msg.Partition, *msg.Offset)

	ma := &api.Offset{
		Topic:     msg.Topic,
		Partition: *msg.Partition,
		Offset:    *msg.Offset,
	}
	if err := c.api.Seek(ma, 100*time.Millisecond); err != nil {
		return ApiError{"Seek", err}
	}

	// ErrReprocessMessage is detected by the consumer run-loop as a signal that it should
	// not commit the offset for the message (when performing synchronous commits).
	return ErrReprocessMessage
}
