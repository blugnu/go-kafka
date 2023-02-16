package kafka

import (
	"fmt"
)

type DeliveryFailure struct {
	error
}

func (e DeliveryFailure) Error() string {
	return fmt.Sprintf("delivery failed: %s", e.error)
}
