package api

import "fmt"

type Error string

func (s Error) Error() string { return string(s) }

const ErrNoTopics = Error("no topics specified")
const ErrTimeout = Error("timeout")

type UnexpectedRebalanceEventError struct {
	Event string
}

func (e UnexpectedRebalanceEventError) Error() string {
	return fmt.Sprintf("unexpected rebalance event: %s", e.Event)
}
