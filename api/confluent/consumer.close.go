package confluent

func (c *Consumer) Close() error {
	return c.funcs.Close()
}
