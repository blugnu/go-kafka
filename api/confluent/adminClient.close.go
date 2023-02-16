package confluent

func (c *adminClient) Close() {
	c.funcs.Close()
}
