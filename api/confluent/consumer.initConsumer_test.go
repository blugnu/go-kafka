package confluent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blugnu/logger"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

func Test_initConsumer(t *testing.T) {
	// ARRANGE
	logsFuncCalled := false
	readMessageCalled := false
	seekCalled := false
	commitCalled := false
	var seekTo confluent.TopicPartition
	var commitAt confluent.TopicPartition

	ctx := context.Background()

	c := &Consumer{funcs: &ConsumerFuncs{},
		Log: &logger.Base{Context: ctx, Adapter: &logger.LogrusAdapter{Logger: logrus.New()}},
	}
	c.funcs.Logs = func() chan confluent.LogEvent {
		logsFuncCalled = true

		ch := make(chan confluent.LogEvent)
		close(ch)

		return ch
	}
	c.funcs.ReadMessage = func(d time.Duration) (*confluent.Message, error) {
		readMessageCalled = true
		return nil, confluent.NewError(confluent.ErrTimedOut, "time out", false)
	}
	c.funcs.Seek = func(tp confluent.TopicPartition, i int) error { seekCalled = true; seekTo = tp; return nil }
	c.funcs.CommitOffsets = func(tp []confluent.TopicPartition) ([]confluent.TopicPartition, error) {
		commitCalled = true
		commitAt = tp[0]
		return nil, nil
	}

	// ACT
	initConsumer(c)
	time.Sleep(time.Millisecond) // Logs is called in a goroutine, so give it time...

	// ASSERT
	t.Run("polls logs channel", func(t *testing.T) {
		wanted := true
		got := logsFuncCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("calls funcs.ReadMessage()", func(t *testing.T) {
		wanted := true
		got := readMessageCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns error from funcs.ReadMessage()", func(t *testing.T) {
		// ARRANGE
		rmerr := errors.New("read message error")
		ofn := c.funcs.ReadMessage
		defer func() { c.funcs.ReadMessage = ofn }()
		c.funcs.ReadMessage = func(d time.Duration) (*confluent.Message, error) { return nil, rmerr }

		// ACT
		err := initConsumer(c)
		time.Sleep(time.Millisecond) // Logs is called in a goroutine, so give it time...

		// ASSERT
		wanted := rmerr
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})

	t.Run("when no message is read", func(t *testing.T) {
		t.Run("does not call funcs.Seek()", func(t *testing.T) {
			wanted := true
			got := !seekCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
		t.Run("does not call funcs.Commit()", func(t *testing.T) {
			wanted := true
			got := !commitCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("when a message is read", func(t *testing.T) {
		// ARRANGE
		c.funcs.ReadMessage = func(d time.Duration) (*confluent.Message, error) {
			t := "topic"
			return &confluent.Message{
				TopicPartition: confluent.TopicPartition{
					Topic:     &t,
					Partition: 1,
					Offset:    42,
				},
			}, nil
		}

		t.Run("returns error from funcs.Seek()", func(t *testing.T) {
			// ARRANGE
			seekerr := errors.New("seek error")
			ofn := c.funcs.Seek
			defer func() { c.funcs.Seek = ofn }()
			c.funcs.Seek = func(tp confluent.TopicPartition, i int) error { return seekerr }

			// ACT
			err := initConsumer(c)
			time.Sleep(time.Millisecond) // Logs is called in a goroutine, so give it time...

			// ASSERT
			wanted := seekerr
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("returns error from funcs.Commit()", func(t *testing.T) {
			// ARRANGE
			commiterr := errors.New("commit error")
			ofn := c.funcs.CommitOffsets
			defer func() { c.funcs.CommitOffsets = ofn }()
			c.funcs.CommitOffsets = func(tp []confluent.TopicPartition) ([]confluent.TopicPartition, error) { return nil, commiterr }

			// ACT
			err := initConsumer(c)
			time.Sleep(time.Millisecond) // Logs is called in a goroutine, so give it time...

			// ASSERT
			wanted := commiterr
			got := err
			if wanted != got {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		})

		t.Run("seeks to re-read message", func(t *testing.T) {
			topic := "topic"

			// ACT
			initConsumer(c)
			time.Sleep(time.Millisecond) // Logs is called in a goroutine, so give it time...

			// ASSERT
			t.Run("calls funcs.Seek()", func(t *testing.T) {
				wanted := true
				got := seekCalled
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}

				t.Run("with offset", func(t *testing.T) {
					wanted := confluent.TopicPartition{
						Topic:     &topic,
						Partition: 1,
						Offset:    42,
					}
					got := seekTo
					if *wanted.Topic != *got.Topic ||
						wanted.Partition != got.Partition ||
						wanted.Offset != got.Offset {
						t.Errorf("\nwanted %v\ngot    %v", wanted, got)
					}
				})
			})

			t.Run("calls funcs.Commit()", func(t *testing.T) {
				wanted := true
				got := commitCalled
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}

				t.Run("with offset", func(t *testing.T) {
					wanted := confluent.TopicPartition{
						Topic:     &topic,
						Partition: 1,
						Offset:    42,
					}
					got := commitAt
					if *wanted.Topic != *got.Topic ||
						wanted.Partition != got.Partition ||
						wanted.Offset != got.Offset {
						t.Errorf("\nwanted %v\ngot    %v", wanted, got)
					}
				})
			})
		})
	})
}
