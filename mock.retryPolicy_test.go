package kafka

import (
	"fmt"
	"time"
)

type MockRetryPolicy struct {
	MaxRetries              int
	shouldRetryCalled       bool
	deferRetryMessageCalled bool
}

func (p MockRetryPolicy) String() string {
	return fmt.Sprintf("Mock Retry Policy (Max. %d retries)", p.MaxRetries)
}

func (p *MockRetryPolicy) ShouldRetry(msg *Message) bool {
	p.shouldRetryCalled = true

	if p.MaxRetries == 0 {
		return false
	}

	_, _, retryNum := msg.IsRetryAttempt()
	return retryNum <= p.MaxRetries
}

func (p *MockRetryPolicy) RetryDelay(msg *Message) time.Duration {
	p.deferRetryMessageCalled = true
	return 1 * time.Second
}
