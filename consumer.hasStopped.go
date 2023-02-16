package kafka

func (c *Consumer) HasStopped() bool {
	return c.state > csRunning
}
