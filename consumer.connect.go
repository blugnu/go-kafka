package kafka

import "context"

// connect establishes a Consumer connection with the underlying api.
func (c *Consumer) connect(ctx context.Context) error {
	// if c.noConnection {
	// 	return nil
	// }

	log := c.log.LogWithContext(ctx)
	cfg := c.config()

	if err := c.api.Create(cfg); err != nil {
		return ApiError{"Create", err}
	}

	log.Debug("consumer connected")
	return nil
}
