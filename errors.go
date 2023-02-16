package kafka

type Error string

func (s Error) Error() string { return string(s) }

const ErrNoEncryptionHandler = Error("no encryption handler is configured.  Configure a handler for the topic or the consumer")
const ErrNoDefaultProducer = Error("a DefaultProducer has not been configured")
const ErrNoDeferralHandler = Error("no deferral handler is configured.  Configure a handler for the topic or the consumer")
const ErrNoMessageHandlers = Error("no message handlers defined")
const ErrNoMessageProducer = Error("this consumer requires a MessageProducer.  Configure a DefaultProducer or provide a MessageProducer for the consumer")
const ErrNoHandlerFunc = Error("handler func not defined")
const ErrNoRetryPolicy = Error("no retry policy.  Configure a policy for the topic or the consumer")
const ErrNoTopicId = Error("message topic is not set")
const ErrNoTopics = Error("no topics specified")
const ErrNotConnected = Error("not connected")
const ErrReprocessMessage = Error("reprocess message")
const ErrTimeout = Error("timed out")
