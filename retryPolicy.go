package kafka

import "time"

type RetryPolicy interface {
	ShouldRetry(*Message) bool
	RetryDelay(*Message) time.Duration
}
