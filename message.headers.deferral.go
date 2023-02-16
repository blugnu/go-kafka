package kafka

import "time"

func (msg *Message) DeferFor(d time.Duration) {
	msg.SetHeader("blugnu/kafka:deferred", d.String())
}

func (msg *Message) isDeferred() (bool, time.Duration) {
	dh, headerSet := msg.Headers["blugnu/kafka:deferred"]
	if !headerSet {
		return false, 0
	}
	d, _ := time.ParseDuration(dh)
	return true, d
}
