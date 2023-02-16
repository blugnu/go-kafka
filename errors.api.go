package kafka

import "fmt"

type ApiError struct {
	operation string
	error
}

func (err ApiError) Error() string {
	if err.operation != "" {
		return fmt.Sprintf("api.%s() error: %v", err.operation, err.error)
	}
	return fmt.Sprintf("api error: %v", err.error)
}

func (err ApiError) Is(target error) bool {
	return err == target
}

func (err ApiError) Unwrap() error {
	return err.error
}
