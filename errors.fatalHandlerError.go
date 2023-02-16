package kafka

import "fmt"

// FatalHandlerError indicates a non-recoverable error; i.e. there is no point retrying the message
// and sending to any dead-letter topic would be inappropriate/undesirable.  Returning an error of
// this type from a message handler will cause the consumer for that message topic to terminate
// without committing the message offset without producing any retry or dead-letter message even if
// retry and dead-letter topics are configured.
//
// i.e. any retry/dead-letter mechanism is suppressed when a FatalHandlerError is returned.
//
// Unlike a HandlerError which is automatically wrapped around any error returned from your handler
// function, you must EXPLICITLY return a FatalHandlerError when required.  To aid diagnosis of the
// fault from logs, you should provide a reason explaining why the error is non-recoverable, e.g.:
//
//	if err := json.Unmarshal(b, t); err != nil {
//	    return FatalHandlerError{"unable to unmarshal the thingumabob", err}
//	}
//
// Consequences:
//
// When a consumer terminates in response to an error of this type, the consumer group will be
// rebalanced; another consumer in the group will then pick up the uncommitted message; if the
// message continues to fail with a fatal error then that consumer will also terminate.
//
// In a scenario where only one message is failing and all other messages in a topic do not trigger
// the same or similar error, other partitions will continue to be processed; but the partition
// containing the failing message is effectively blocked.
//
// THIS IS BY DESIGN.
//
// A FatalHandlerError should ONLY be returned where the cause of the error is unrecoverable, representing
// a failure in the consumer implementation and requiring the deployment of an updated consumer.  The
// mechanism ensures that such messages are not "lost" and will eventually be processed when a consumer
// is deployed with a remediation to enable the message to be processed appropriately.
//
// For example, if unmarshalling the message payload is impossible (e.g. due to a version mis-match between the
// message payload version and the consumer version supporting that payload), requiring an updated consumer
// to be deployed in order to process the message.
//
// In this scenario, during a rolling deployment of an updated producer/consumer where a message payload schema
// has changed, an updated producer might start producing messages that existing consumers are unable to handle.
// By returning a FatalHandlerError, those consumers are terminated and are replaced (eventually, if not
// immediately) by the updated consumer which *is* then able to handle the message.
//
// In most cases (typically involving external dependencies, e.g. where handling a message involves calling
// some other RESTful service or a database) and that external dependency fails, it is more appropriate to
// return some other error type and configure an appropriate retry/dead-letter mechanism to keep messages
// flowing through your system without loss.
type FatalHandlerError struct {
	Reason string
	E      error
}

func (err FatalHandlerError) Error() string {
	return fmt.Sprintf("fatal error: %s: %s", err.Reason, err.E)
}

func (err FatalHandlerError) Is(target error) bool {
	return err == target
}

func (err FatalHandlerError) Unwrap() error {
	return err.E
}
