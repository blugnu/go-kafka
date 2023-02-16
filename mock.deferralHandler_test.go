package kafka

import (
	"context"
)

type MockDeferralHandler struct {
	wasCalled bool
	err       error
}

func (h *MockDeferralHandler) Defer(context.Context, *Consumer, *Message) error {
	h.wasCalled = true
	return h.err
}
