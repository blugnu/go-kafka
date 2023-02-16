package kafka

import (
	"fmt"
	"time"
)

type ExponentialBackoffRetryPolicy struct {
	InitialWait time.Duration
	MaxWait     time.Duration
	MaxRetries  int
}

func (p ExponentialBackoffRetryPolicy) String() string {
	w := p.InitialWait.String()
	if p.MaxWait > 0 {
		w = fmt.Sprintf("%s-%s", w, p.MaxWait)
	}
	if p.MaxRetries == 1 {
		return fmt.Sprintf("ExponentialBackoff: Wait %s (1 retry)", w)
	}
	return fmt.Sprintf("ExponentialBackoff: Wait %s (max %d retries)", w, p.MaxRetries)
}

func (p *ExponentialBackoffRetryPolicy) ShouldRetry(msg *Message) bool {
	if p.MaxRetries == -1 {
		return true
	}

	_, _, retryNum := msg.IsRetryAttempt()
	return retryNum <= p.MaxRetries
}

func (p *ExponentialBackoffRetryPolicy) RetryDelay(msg *Message) time.Duration {
	_, _, num := msg.IsRetryAttempt()

	if p.MaxWait == 0 {
		return p.InitialWait
	}

	d := p.InitialWait
	for i := 1; i < num && d < p.MaxWait; i++ {
		d *= 2
	}
	if d > p.MaxWait {
		d = p.MaxWait
	}

	return d
}
