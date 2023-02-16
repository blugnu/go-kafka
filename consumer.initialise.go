package kafka

import (
	"context"
	"os"
	"os/signal"

	"github.com/blugnu/kafka/api/confluent"
)

// TODO: if configured with a RetryPolicy but no RetryTopic, produce retry messages to the original topic

// initialise ensures that all required configuration has been provided and
// establishes any defaults that may be required for any optional configuration
// not explicitly supplied.
//
// - configures the base Log for the Consumer
//
// - Assigns the confluent Consumer Api if no Api is configured
//
// - Assigns the NoEncryption handler if no EncryptionHandler is configured
//
// - Ensures that a RetryPolicy is specified for any message handler with a RetryTopic
//
// - Adds a RetryTopic handler map for message handlers with a RetryTopic
func (c *Consumer) initialise(ctx context.Context) error {
	// There must be at least one message handler configured
	if len(c.MessageHandlers) == 0 {
		return ConfigurationError{error: ErrNoMessageHandlers}
	}

	c.log = getBaseLogger(c.Log, ctx)
	log := c.logEntry()

	// If no Api is configured, apply the confluent.Consumer Api
	c.api = c.Api
	if c.api == nil {
		c.api = &confluent.Consumer{Log: c.log}
	}
	log.Tracef("using api %T", c.api)

	// If no EncryptionHandler is configured, apply the default
	if c.EncryptionHandler == nil {
		c.EncryptionHandler = Encryption
	}
	if c.EncryptionHandler == nil {
		return ConfigurationError{error: ErrNoEncryptionHandler}
	}
	log.Tracef("using encryption handler %T", c.EncryptionHandler)

	producerRequired := false

	// Check message handlers for any with a RetryTopic but no RetryPolicy
	c.handlers = MessageHandlerMap{}
	for k, v := range c.MessageHandlers {
		if v.Func == nil {
			return ConfigurationError{Topic: k, error: ErrNoHandlerFunc}
		}

		producerRequired = producerRequired || v.RetryTopic != "" || v.DeadLetterTopic != ""

		// If no RetryTopic is configured then there is no further initialisation
		// required for this handler
		if v.RetryTopic == "" {
			c.handlers[k] = v
			continue
		}

		// Since a RetryTopic has been specified ...

		// ... a RetryPolicy MUST also be configured either on the handler itself or on the
		// consumer (to be applied to all handlers with retry topics)
		if v.RetryPolicy == nil {
			v.RetryPolicy = c.RetryPolicy
		}
		if v.RetryPolicy == nil {
			return ConfigurationError{Topic: k, error: ErrNoRetryPolicy}
		}

		// ... a DeferralHandler MUST also be configured either on the handler itself or on the
		// consumer (to be applied to all handlers with retry topics)
		if v.DeferralHandler == nil {
			v.DeferralHandler = c.DeferralHandler
		}
		if v.DeferralHandler == nil {
			return ConfigurationError{Topic: k, error: ErrNoDeferralHandler}
		}

		// Map the configured handler to the required topic
		c.handlers[k] = v

		// If the RetryTopic is not the same as the origin topic then the handler must be mapped
		// for the RetryTopic as well as the origin topic, so that retry messages are also handled
		if v.RetryTopic != k {
			c.handlers[v.RetryTopic] = v
		}
	}

	if producerRequired {
		if c.MessageProducer == nil {
			c.MessageProducer = DefaultProducer
		}
		if c.MessageProducer == nil {
			return ConfigurationError{error: ErrNoMessageProducer}
		}
	}

	// Setup ctrl-c signal channel for when running in interactive console
	c.ctrlc = make(chan os.Signal, 1)
	signal.Notify(c.ctrlc, os.Interrupt)

	log.Debug("consumer initialised")
	return nil
}
