package kafka

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/api/mock"
)

func TestConsumer_ConsumeMessagesWithSynchronousCommits(t *testing.T) {
	testcases := []struct {
		name       string
		async      bool
		numCommits int
	}{
		{name: "enabled", async: false, numCommits: 3},
		{name: "disabled", async: true, numCommits: 0},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			ctx := context.Background()
			defer InstallTestLogger(ctx)()

			offset := api.Offset{
				Topic:     "topic",
				Partition: 1,
				Offset:    1,
			}

			capi, mock := mock.ConsumerApi()
			mock.Messages([]any{
				&api.Message{
					Topic:     offset.Topic,
					Partition: &offset.Partition,
					Offset:    &offset.Offset,
				},
				&api.Message{
					Topic:     offset.Topic,
					Partition: &offset.Partition,
					Offset:    &offset.Offset,
				},
				&api.Message{
					Topic:     offset.Topic,
					Partition: &offset.Partition,
					Offset:    &offset.Offset,
				},
			})
			commitCallCount := 0
			mock.Funcs.Commit = func(o *api.Offset, ci api.CommitIntent) error {
				t.Run(fmt.Sprintf("committed intent %d", commitCallCount), func(t *testing.T) {
					wanted := api.ReadNext
					got := ci
					if wanted != got {
						t.Errorf("wanted %v, got %v", wanted, got)
					}
				})
				commitCallCount++
				return nil
			}

			sut := &Consumer{
				Api:         capi,
				Log:         Log,
				AsyncCommit: tc.async,
				MessageHandlers: MessageHandlerMap{
					"topic": MessageHandler{
						Func: func(context.Context, *Message) error { return nil },
					},
				},
				EncryptionHandler: &MockEncryptionHandler{},
			}
			sut.initialise(ctx)

			// ACT
			sut.consumeMessages(ctx)

			// ASSERT
			t.Run("no. of commit calls", func(t *testing.T) {
				wanted := tc.numCommits
				got := commitCallCount
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})

			t.Run("final consumer state", func(t *testing.T) {
				wanted := csClosed
				got := sut.state
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})
		})
	}
}

func TestConsumer_ConsumeMessagesWhenCommitFails(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	offset := api.Offset{
		Topic:     "topic",
		Partition: 1,
		Offset:    1,
	}

	capi, mock := mock.ConsumerApi()
	mock.Messages([]any{
		&api.Message{
			Topic:     offset.Topic,
			Partition: &offset.Partition,
			Offset:    &offset.Offset,
		},
	})
	commiterr := errors.New("error committing offset")
	mock.Funcs.Commit = func(o *api.Offset, ci api.CommitIntent) error { return commiterr }

	sut := &Consumer{
		Api: capi,
		Log: Log,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { return nil },
			},
		},
		EncryptionHandler: &MockEncryptionHandler{},
	}
	sut.initialise(ctx)

	// ACT
	sut.consumeMessages(ctx)

	// ASSERT
	t.Run("final consumer state", func(t *testing.T) {
		wanted := csClosed
		got := sut.state
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("consumer errors", func(t *testing.T) {
		{
			wanted := 1
			got := len(sut.errors)
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		}
		{
			wanted := ApiError{"Commit", commiterr}
			got := sut.errors[0]
			if !errors.Is(got, wanted) {
				t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
			}
		}
	})
}

// This test fakes a Ctrl-C break of the consumer message loop.  A single message
// is mocked but consumeMessages is expected to respond to the ctrl-c signal (sent
// to the ctrlc chan before called consumeMessages) rather than process any messages.
//
// NOTE: It is to facilitate this test that the ctrlc channel is maintained
// as a field of the Consumer struct rather than being a local variable in the
// consumeMessages function itself
func TestConsumer_ConsumeMessagesWhenCtrlCSignalled(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	defer InstallTestLogger(ctx)()

	offset := api.Offset{
		Topic:     "topic",
		Partition: 1,
		Offset:    1,
	}

	capi, mock := mock.ConsumerApi()
	mock.Messages([]any{
		&api.Message{
			Topic:     offset.Topic,
			Partition: &offset.Partition,
			Offset:    &offset.Offset,
		},
	})
	msgsHandled := 0

	sut := &Consumer{
		Api: capi,
		Log: Log,
		MessageHandlers: MessageHandlerMap{
			"topic": MessageHandler{
				Func: func(context.Context, *Message) error { msgsHandled++; return nil },
			},
		},
		EncryptionHandler: &MockEncryptionHandler{},
	}
	sut.initialise(ctx)

	// Immediately fake a ctrlc signal
	sut.ctrlc <- syscall.Signal(2)

	// ACT
	sut.consumeMessages(ctx)

	// ASSERT
	t.Run("final consumer state", func(t *testing.T) {
		wanted := csClosed
		got := sut.state
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("consumer errors", func(t *testing.T) {
		wanted := 0
		got := len(sut.errors)
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("messages handled", func(t *testing.T) {
		wanted := 0
		got := msgsHandled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}
