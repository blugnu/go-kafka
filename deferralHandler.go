package kafka

import (
	"context"
)

type DeferralHandler interface {
	Defer(context.Context, *Consumer, *Message) error
}
