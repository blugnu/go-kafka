package kafka

import "fmt"

type EncryptionHandlerError struct {
	error
}

func (err EncryptionHandlerError) Error() string {
	return fmt.Sprintf("encryption handler error: %v", err.error)
}

func (err EncryptionHandlerError) Is(target error) bool {
	return err == target
}

func (err EncryptionHandlerError) Unwrap() error {
	return err.error
}
