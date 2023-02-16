package kafka

import "fmt"

type ProducerError struct {
	error
}

func (err ProducerError) Error() string {
	return fmt.Sprintf("producer error: %v", err.error)
}

func (err ProducerError) Is(target error) bool {
	return err == target
}

func (err ProducerError) Unwrap() error {
	return err.error
}
