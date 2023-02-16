package kafka

import "time"

func (p *Producer) Flush(timeout time.Duration) int {
	if timeout < time.Millisecond {
		timeout = 0
	}
	return p.api.Flush(int(timeout.Milliseconds()))
}
