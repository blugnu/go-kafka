package kafka

import (
	"context"
)

func (r *Reader) connect(ctx context.Context) error {
	log := getBaseLogger(r.Log, ctx).Log()

	if err := r.initialise(ctx); err != nil {
		return ConfigurationError{error: err}
	}

	cfg := r.config()
	if err := r.api.Create(cfg); err != nil {
		return ApiError{"Create", err}
	}

	log.Trace("connected")
	r.state = rsConnected

	return nil
}
