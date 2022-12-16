package kafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ErrorCallback func(context.Context, *kafka.Message, error) bool

type CommitError struct {
	error
	*kafka.Message
}

func (e CommitError) Error() string {
	return fmt.Sprintf("commit error: %v", e.error)
}

func (e CommitError) Unwrap() error {
	return e.error
}

type ConfigurationError struct {
	error
}

func (e ConfigurationError) Error() string {
	return fmt.Sprintf("configuration error: %v", e.error)
}

type ConfluentApiError struct {
	error
}

func (e ConfluentApiError) Error() string {
	return fmt.Sprintf("confluent api error: %v", e.error)
}

type FatalError struct {
	error
	*kafka.Message
}

func (e FatalError) Error() string {
	return fmt.Sprintf("fatal error: %v", e.error)
}

func (e FatalError) Unwrap() error {
	return e.error
}

type HandlerError struct {
	error
	*kafka.Message
}

func (e HandlerError) Error() string {
	return fmt.Sprintf("handler error: %v", e.error)
}

func (e HandlerError) Unwrap() error {
	return e.error
}

type MiddlewareError struct {
	error
	*kafka.Message
}

func (e MiddlewareError) Error() string {
	return fmt.Sprintf("middleware error: %v", e.error)
}

func (e MiddlewareError) Unwrap() error {
	return e.error
}

type ReadError struct {
	error
	*kafka.Message
}

func (e ReadError) Error() string {
	return fmt.Sprintf("read error: %v", e.error)
}

func (e ReadError) Unwrap() error {
	return e.error
}

type UnexpectedDeliveryEvent struct {
	event kafka.Event
}

func (e UnexpectedDeliveryEvent) Error() string {
	return fmt.Sprintf("unexpected delivery event (%T): %[1]s", e.event)
}

type ErrNoTopicId struct {
	message string
}

func (e ErrNoTopicId) Error() string {
	return "message had no topic id"
}
