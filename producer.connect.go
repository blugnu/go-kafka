package kafka

import "context"

// Connect initialises the producer, ensuring it has a valid configuration.
//
// If successful, a connection with the broker is then established, making
// the producer ready to produce messages.
//
// Will return an error if initialisation fails or if connection to the
// broker is not established.
func (p *Producer) Connect(ctx context.Context) error {
	if p.state != psInitialising {
		return InvalidStateError{"Connect", p.state.String()}
	}

	p.initialise(ctx)

	return p.connect(ctx)
}

// connect connects to the broker
func (p *Producer) connect(ctx context.Context) error {
	log := p.getLog()

	log.Trace("configuring producer")
	cfg := p.config()

	log.Trace("creating producer")
	if err := p.api.Create(cfg); err != nil {
		return ApiError{"Create", err}
	}

	log.Debug("producer connected")

	p.state = psConnected
	return nil
}
