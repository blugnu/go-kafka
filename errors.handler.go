package kafka

import "fmt"

// HandlerError is a simple wrapper error that is automatically wrapped around any
// error returned from a message handler function.  You do not need to wrap errors
// from you handler function explicitly.
//
// If your handler function experiences a terminal, non-recoverable error and wishes to
// shutdown the consumer without committing the message that resulted in the error
// and without producing any Retry or Dead-Letter message, it must explicitly return
// a FatalHandlerError.
type HandlerError struct {
	error
}

func (err HandlerError) Error() string {
	return fmt.Sprintf("handler error: %v", err.error)
}

func (err HandlerError) Is(target error) bool {
	return err == target
}

func (err HandlerError) Unwrap() error {
	return err.error
}
