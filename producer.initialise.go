package kafka

import (
	"context"

	"github.com/blugnu/kafka/api/confluent"
)

func (p *Producer) initialise(ctx context.Context) {
	p.log = getBaseLogger(p.Log, ctx)
	log := p.getLog()

	p.api = p.Api
	if p.api == nil {
		p.api = &confluent.Producer{}
	}

	// If no EncryptionHandler is configured, apply the default
	// If no default is configured, assume NoEncryption
	if p.EncryptionHandler == nil {
		p.EncryptionHandler = Encryption
	}
	if p.EncryptionHandler == nil {
		p.EncryptionHandler = &NoEncryption{}
	}
	log.Tracef("using encryption handler %T", p.EncryptionHandler)

	log.Debug("producer initialised")
}
