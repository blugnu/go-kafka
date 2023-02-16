package kafka

import (
	"os"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/logger"
)

type Consumer struct {
	MessageHandlers MessageHandlerMap
	EncryptionHandler
	DeferralHandler
	RetryPolicy
	MessageProducer
	AsyncCommit bool
	GroupId     string
	Api         api.Consumer
	Log         *logger.Base
	OnRebalance api.RebalanceEventHandler
	api         api.Consumer
	handlers    MessageHandlerMap
	log         *logger.Base
	state       consumerState
	//	noConnection bool // used in tests (only)
	ctrlc  chan os.Signal
	errors []error
}

func (c *Consumer) logEntry() *logger.Log {
	return c.log.Log()
}

func (c *Consumer) Errors() []error {
	return append([]error{}, c.errors...)
}
