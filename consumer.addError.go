package kafka

func (c *Consumer) addError(err error, state consumerState) error {
	c.errors = append(c.errors, err)
	c.state = state
	return err
}
