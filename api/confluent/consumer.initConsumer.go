package confluent

import (
	"time"

	"github.com/blugnu/kafka/api"
)

var initConsumer = func(c *Consumer) error {
	// Having requested librd logs we must poll the logs channel
	// to prevent it filling up; a go routine to silently read
	// events from that channel until closed will do that job
	go func() {
		for {
			if _, ok := <-c.funcs.Logs(); !ok {
				return
			}
		}
	}()

	// NewConsumer doesn't actually attempt any communication with the broker
	// so we can't be certain that we have a valid consumer at this point!
	//
	// One way to be sure is to attempt to read a message.
	//
	// We will either get an error (bad connection) or a timeout (good connection
	// but no messages waiting) or we might actually read a message in which case
	// we seek back to it and commit, to ensure it will be re-read once the real
	// read loop is started
	if msg, err := c.ReadMessage(100 * time.Millisecond); err != nil {
		if err == api.ErrTimeout {
			return nil
		}
		return err
	} else {
		offset := &api.Offset{
			Topic:     msg.Topic,
			Partition: *msg.Partition,
			Offset:    *msg.Offset,
		}
		if err := c.Seek(offset, 100*time.Millisecond); err != nil {
			return err
		}
		if err := c.Commit(offset, api.ReadAgain); err != nil {
			return err
		}
	}

	return nil
}
