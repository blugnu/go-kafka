package kafka

import "fmt"

type InvalidStateError struct {
	operation string
	state     string
}

func (e InvalidStateError) Error() string {
	return fmt.Sprintf("operation %s() invalid in state %s", e.operation, e.state)
}

func (e InvalidStateError) Is(target error) bool {
	return e == target
}
