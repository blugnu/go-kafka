package kafka

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const noBrokerBootstrap = "test://nobroker"

type config struct {
	api        interface{}
	config     configMap
	middleware MessageHandler
	ecb        ErrorCallback
	rcb        kafka.RebalanceCb
	// Consumer-only members
	messageHandlers messageHandlerMap // map of topic-name:handler
	topicHandlers   topicHandlerMap   // map of topic-name:handler
	readCommitted   bool
}

func NewConfig() *config {
	return &config{
		config:          configMap{},
		messageHandlers: messageHandlerMap{},
	}
}

func (c *config) copy() *config {
	return &config{
		api:             c.api,
		config:          c.config.copy(),
		middleware:      c.middleware,
		messageHandlers: c.messageHandlers.copy(),
		readCommitted:   c.readCommitted,
		ecb:             c.ecb,
		rcb:             c.rcb,
	}
}

func (c *config) autoCommit() bool {
	enabled, ok := c.config[key[enableAutoCommit]]
	return !ok || enabled.(bool)
}

func (c *config) HasNoBroker() bool {
	bss, ok := c.config[key[bootstrapServers]]
	return ok && bss == noBrokerBootstrap
}

func (c *config) With(key string, value interface{}) *config {
	r := c.copy()
	r.config[key] = value
	return r
}

func (c *config) Using(api interface{}) *config {
	r := c.copy()
	r.api = api
	return r
}

func (c *config) WithAutoCommit(v bool) *config {
	if v && c.readCommitted {
		panic("cannot enable AutoCommit with ReadCommitted also enabled")
	}

	r := c.copy()
	r.config[key[enableAutoCommit]] = v
	return r
}

func (c *config) WithBatchSize(size int) *config {
	r := c.copy()
	r.config[key[batchSize]] = size
	return r
}

func (c *config) WithBootstrapServers(servers interface{}) *config {
	r := c.copy()

	switch servers := servers.(type) {
	case []string:
		r.config[key[bootstrapServers]] = strings.Join(servers, ",")
	case string:
		r.config[key[bootstrapServers]] = servers
	default:
		panic(fmt.Sprintf("servers is %T: must be string or []string", servers))
	}

	return r
}

func (c *config) WithErrorCallback(ecb ErrorCallback) *config {
	r := c.copy()
	r.ecb = ecb
	return r
}

func (c *config) WithMiddleware(middleware MessageHandler) *config {
	r := c.copy()
	r.middleware = middleware
	return r
}

// WithNoBroker returns a Config configured to prevent Consumer or Provider
// initialisation from connecting a client to any broker.  This is
// intended for use in TESTS only.
//
// Attempting to use a Consumer or Producer configured with this setting is
// unsupported and is likely to result in errors, panics or other
// unpredictable behaviour.
//
// This has limited use cases but is necessary to test certain aspects of the
// Consumer and Producer client hooking mechanism.
//
// For comprehensive mocking, faking and stubbing use WithHooks().
func (c *config) WithNoBroker() *config {
	return c.WithBootstrapServers(noBrokerBootstrap)
}

func (c *config) WithGroupId(s string) *config {
	r := c.copy()
	r.config[key[groupId]] = s
	return r
}

func (c *config) WithIdempotence(v bool) *config {
	r := c.copy()
	r.config[key[enableIdempotence]] = v
	return r
}

func (c *config) WithReadCommitted(v bool) *config {
	if v && c.autoCommit() {
		panic("cannot enable ReadCommitted with AutoCommit also enabled")
	}

	r := c.copy()
	r.config[key[enableAutoCommit]] = v
	return r
}

func (c *config) WithMessageHandler(t string, fn MessageHandler) *config {
	r := c.copy()
	r.messageHandlers[t] = fn
	return r
}

func (c *config) WithTopicHandler(t string, h TopicHandler) *config {
	r := c.copy()
	r.topicHandlers[t] = h
	return r
}
