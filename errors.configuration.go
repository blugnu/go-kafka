package kafka

import "fmt"

type ConfigurationError struct {
	Topic string
	error
}

func (err ConfigurationError) Error() string {
	if err.Topic != "" {
		return fmt.Sprintf("%s configuration error: %v", err.Topic, err.error)
	}
	return fmt.Sprintf("configuration error: %v", err.error)
}

func (err ConfigurationError) Is(target error) bool {
	return err == target
}

func (err ConfigurationError) Unwrap() error {
	return err.error
}
