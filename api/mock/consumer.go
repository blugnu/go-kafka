package mock

import (
	"errors"
	"fmt"
	"time"

	"github.com/blugnu/kafka/api"
)

type consumerFuncs struct {
	Assign    func([]string) error
	Create    func(*api.ConfigMap) error
	Close     func() error
	Commit    func(*api.Offset, api.CommitIntent) error
	Seek      func(*api.Offset, time.Duration) error
	Subscribe func([]string, api.RebalanceEventHandler) error
}

type mockConsumer struct {
	AssignWasCalled        bool
	AssignWasCalledWithNil bool
	CloseWasCalled         bool
	CommitWasCalled        bool
	CreateWasCalled        bool
	ResetWasCalled         bool
	SeekWasCalled          bool
	SubscribeWasCalled     bool
	Config                 *api.ConfigMap
	Funcs                  *consumerFuncs
	CommitOffset           *api.Offset
	SeekOffset             *api.Offset
	TopicsAssigned         []string
	TopicsSubscribed       []string
	messages               []any
	messageIndex           int
	messageRead            bool
}

func ConsumerApi() (api.Consumer, *mockConsumer) {
	result := &mockConsumer{}
	result.Reset()

	return result, result
}

func (c *mockConsumer) Messages(msgs []any) {

	for _, msg := range msgs {
		switch msg.(type) {
		case api.Message:
			continue
		case *api.Message:
			continue
		case error:
			continue
		}
		panic(fmt.Sprintf("unsupported item of type %T; only (*)api.message or error expected)", msg))
	}

	c.messages = append(c.messages, msgs...)
}

func (c *mockConsumer) Assign(topics []string) error {
	c.AssignWasCalled = true
	c.AssignWasCalledWithNil = topics == nil
	c.TopicsAssigned = append(c.TopicsAssigned, topics...)

	return c.Funcs.Assign(topics)
}

func (c *mockConsumer) Close() error {
	c.CloseWasCalled = true
	return c.Funcs.Close()
}

func (c *mockConsumer) Create(cfg *api.ConfigMap) error {
	c.CreateWasCalled = true
	c.Config = cfg
	return c.Funcs.Create(cfg)
}

func (c *mockConsumer) Commit(offset *api.Offset, intent api.CommitIntent) error {
	c.CommitWasCalled = true
	c.CommitOffset = offset

	switch intent {
	case api.ReadAgain:
		if c.messageRead && c.messageIndex > 0 {
			c.messageIndex -= 1
		}
	case api.ReadNext:
		// NO-OP
	}
	c.messageRead = false

	return c.Funcs.Commit(offset, intent)
}

func (c *mockConsumer) Seek(offset *api.Offset, timeout time.Duration) error {
	c.SeekWasCalled = true
	c.SeekOffset = offset
	return c.Funcs.Seek(offset, timeout)
}

func (c *mockConsumer) Subscribe(topics []string, rb api.RebalanceEventHandler) error {
	c.SubscribeWasCalled = true
	c.TopicsSubscribed = topics

	return c.Funcs.Subscribe(topics, rb)
}

func (c *mockConsumer) ReadMessage(timeout time.Duration) (*api.Message, error) {
	if c.messageIndex >= len(c.messages) {
		return nil, ErrNoMoreMessages
	}

	msg := c.messages[c.messageIndex]
	c.messageIndex++
	c.messageRead = true

	switch msg := msg.(type) {
	case api.Message:
		return &msg, nil
	case *api.Message:
		return msg, nil
	case error:
		return nil, msg
	}

	c.messageIndex--
	c.messageRead = false

	// This should not be reachable as the Messages() func for specifying mocked
	// messages should panic if it encounters an unsupported type
	return nil, errors.New("unsupported type for mocked message")
}

func (c *mockConsumer) Reset() {
	c.ResetWasCalled = true

	c.AssignWasCalled = false
	c.AssignWasCalledWithNil = false
	c.CloseWasCalled = false
	c.CommitWasCalled = false
	c.CreateWasCalled = false
	c.SeekWasCalled = false
	c.SubscribeWasCalled = false

	c.Config = nil
	c.TopicsAssigned = nil
	c.TopicsSubscribed = nil

	c.messages = []any{}
	c.messageIndex = 0

	c.Funcs = &consumerFuncs{
		Assign:    func([]string) error { return nil },
		Create:    func(cfg *api.ConfigMap) error { return nil },
		Close:     func() error { return nil },
		Commit:    func(offset *api.Offset, intent api.CommitIntent) error { return nil },
		Seek:      func(offset *api.Offset, timeout time.Duration) error { return nil },
		Subscribe: func(ta []string, rb api.RebalanceEventHandler) error { return nil },
	}
}
