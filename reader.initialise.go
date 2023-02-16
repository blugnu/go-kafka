package kafka

import (
	"context"
	"fmt"

	confluent "github.com/blugnu/kafka/api/confluent"
	"github.com/google/uuid"
)

func (r *Reader) initialise(ctx context.Context) error {
	r.log = getBaseLogger(r.Log, ctx)
	log := r.getLog()

	if len(r.Topics) == 0 {
		return ErrNoTopics
	}

	r.api = r.Api
	if r.api == nil {
		r.api = &confluent.Consumer{Log: r.log}
	}

	if r.EncryptionHandler == nil {
		r.EncryptionHandler = Encryption
	}
	if r.EncryptionHandler == nil {
		r.EncryptionHandler = &NoEncryption{}
	}

	// A group.id must be set but a reader will not subscribe so will not
	// actually join any group.  In theory we could set the group id to
	// anything we like such as "not used", but to be safe we wil generate
	// a (most likely) unique, random group.id for each reader
	r.groupId = fmt.Sprintf("reader-%s", uuid.NewString())

	log.Trace("reader initialised")

	r.state = rsInitialised
	return nil
}
