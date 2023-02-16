package kafka

import (
	"context"

	"github.com/blugnu/kafka/api"
)

var stoppedstate = map[bool]consumerState{
	true:  csStoppedWithError,
	false: csStopped,
}

func (c *Consumer) consumeMessages(ctx context.Context) {
	defer c.close()

	log := c.logEntry()

	// Create a default reader function for handling messages and explicitly
	// committing the message offset if handled without error
	consume := func() error {
		offset, err := c.consumeMessage(ctx)
		if err != nil {
			return err
		}

		log.Trace("committing offset")
		if err := c.api.Commit(offset, api.ReadNext); err != nil {
			return ApiError{"Commit", err}
		}

		return nil
	}

	// If using AsyncCommit, replace the reader function with one that only
	// consumes a messages without committing the offset
	if c.AsyncCommit {
		consume = func() error {
			_, err := c.consumeMessage(ctx)
			return err
		}
	}

	c.state = csRunning

	log.Debug("consumer running")

forLoop:
	for len(c.errors) == 0 {
		select {
		case <-c.ctrlc:
			log.Debug("consumer stopping")
			break forLoop
		default:
			err := consume()
			switch err {
			case ErrReprocessMessage, ErrTimeout, (error)(nil):
				continue
			default:
				log.Tracef("consumer error: %s", err)
				c.errors = append(c.errors, err) // FIXME: error types
			}
		}
	}

	c.state = stoppedstate[len(c.errors) > 0]
	log.Debug("consumer stopped")
}
