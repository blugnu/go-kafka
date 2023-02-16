package kafka

func (c *Consumer) close() error {
	log := c.logEntry()

	log.Trace("consumer closing")
	if err := c.api.Close(); err != nil {
		return c.addError(ApiError{"Close", err}, csCloseFailed)
	}

	log.Debug("consumer closed")
	c.state = csClosed

	return nil
}
