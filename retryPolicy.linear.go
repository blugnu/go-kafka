package kafka

import (
	"fmt"
	"time"
)

type LinearRetryPolicy struct {
	Wait       time.Duration
	MaxRetries int
}

func (p LinearRetryPolicy) String() string {
	if p.MaxRetries == 1 {
		return fmt.Sprintf("Linear: Wait %s (1 retry)", p.Wait)
	}
	return fmt.Sprintf("Linear: Wait %s (max %d retries)", p.Wait, p.MaxRetries)
}

func (p *LinearRetryPolicy) ShouldRetry(msg *Message) bool {
	if p.MaxRetries == -1 {
		return true
	}

	_, _, retryNum := msg.IsRetryAttempt()
	return retryNum <= p.MaxRetries
}

func (p *LinearRetryPolicy) RetryDelay(msg *Message) time.Duration {
	return p.Wait
}
