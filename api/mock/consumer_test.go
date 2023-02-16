package mock

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/set"
)

func TestConsumerApi(t *testing.T) {
	// ARRANGE
	// ACT
	api, mock := ConsumerApi()

	// ASSERT
	t.Run("returns api", func(t *testing.T) {
		wanted := true
		got := api != nil
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("returns mock", func(t *testing.T) {
		wanted := true
		got := mock != nil
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with Reset called", func(t *testing.T) {
			wanted := true
			got := mock.ResetWasCalled
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}

func TestThatConsumerMessagesWithValuesOfSupportedTypes(t *testing.T) {
	// ARRANGE
	msg := api.Message{}
	pmsg := &api.Message{}
	err := errors.New("error")

	_, sut := ConsumerApi()

	// ACT
	sut.Messages([]any{msg, pmsg, err})

	// ASSERT
	t.Run("mocks expected number of messages", func(t *testing.T) {
		wanted := 3
		got := len(sut.messages)
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("mocks expected messages", func(t *testing.T) {
		expected := []any{msg, pmsg, err}
		for i := range sut.messages {
			wa := expected[i]
			ga := sut.messages[i]
			switch wanted := wa.(type) {
			default:
				switch got := ga.(type) {
				case api.Message:
					if !reflect.DeepEqual(wanted, got) {
						t.Errorf("\nat index %d\nwanted %#v\ngot   %#v", i, wanted, got)
					}
				case *api.Message:
					if wanted != got {
						t.Errorf("\nat index %d\nwanted %#v\ngot   %#v", i, wanted, got)
					}
				case error:
					if wanted != got {
						t.Errorf("\nat index %d\nwanted %#v\ngot   %#v", i, wanted, got)
					}
				}
			}
		}
	})
}

func TestThatConsumerMessagesWithValueOfUnsupportedType(t *testing.T) {
	// ARRANGE
	defer func() {
		got := true
		if r := recover(); r == nil {
			got = false
		}

		t.Run("panics", func(t *testing.T) {
			wanted := true
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	}()
	_, sut := ConsumerApi()

	// ACT
	sut.Messages([]any{42})

	// ASSERT
	// see: deferred assert
}

func TestConsumer_AssignWithTopics(t *testing.T) {
	// ARRANGE
	api, mock := ConsumerApi()

	// ACT
	api.Assign([]string{"topic.b", "topic.a"})

	// ASSERT
	t.Run("calls Assign", func(t *testing.T) {
		wanted := true
		got := mock.AssignWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with non-nil topics", func(t *testing.T) {
			wanted := true
			got := !mock.AssignWasCalledWithNil
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("assigns topics", func(t *testing.T) {
			wanted := []string{"topic.a", "topic.b"}
			got := mock.TopicsAssigned
			if !set.FromSlice(wanted).Equal(set.FromSlice(got)) {
				t.Errorf("\nwanted %#v\ngot   %#v", wanted, got)
			}
		})
	})
}

func TestConsumer_AssignWithNoTopics(t *testing.T) {
	// ARRANGE
	api, mock := ConsumerApi()

	// ACT
	api.Assign(nil)

	// ASSERT
	t.Run("calls Assign", func(t *testing.T) {
		wanted := true
		got := mock.AssignWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with nil topics", func(t *testing.T) {
			wanted := true
			got := mock.AssignWasCalledWithNil
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("assigns no topics", func(t *testing.T) {
			wanted := true
			got := len(mock.TopicsAssigned) == 0
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}

func TestConsumer_Close(t *testing.T) {
	// ARRANGE
	api, mock := ConsumerApi()

	// ACT
	api.Close()

	// ASSERT
	t.Run("calls Close", func(t *testing.T) {
		wanted := true
		got := mock.CloseWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func TestConsumer_Commit(t *testing.T) {
	testcases := []struct {
		intent              api.CommitIntent
		messageRead         bool
		initialMessageIndex int
		finalMessageIndex   int
	}{
		{intent: api.ReadAgain, messageRead: true, initialMessageIndex: 1, finalMessageIndex: 0},
		{intent: api.ReadNext, messageRead: true, initialMessageIndex: 1, finalMessageIndex: 1},
		{intent: api.ReadAgain, messageRead: false, initialMessageIndex: 1, finalMessageIndex: 1},
		{intent: api.ReadNext, messageRead: false, initialMessageIndex: 1, finalMessageIndex: 1},
	}
	for _, tc := range testcases {
		t.Run(tc.intent.String(), func(t *testing.T) {
			// ARRANGE
			offset := &api.Offset{Topic: "topic", Partition: 1, Offset: 42}
			api, mock := ConsumerApi()
			mock.messageIndex = tc.initialMessageIndex
			mock.messageRead = tc.messageRead

			// ACT
			api.Commit(offset, tc.intent)

			// ASSERT
			t.Run("calls Commit", func(t *testing.T) {
				wanted := true
				got := mock.CommitWasCalled
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})

			t.Run("adjusts messageIndex", func(t *testing.T) {
				wanted := tc.finalMessageIndex
				got := mock.messageIndex
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})
		})
	}
}

func TestConsumer_Create(t *testing.T) {
	// ARRANGE
	cfg := &api.ConfigMap{}
	api, mock := ConsumerApi()

	// ACT
	api.Create(cfg)

	// ASSERT
	t.Run("calls Create", func(t *testing.T) {
		wanted := true
		got := mock.CreateWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with specified config", func(t *testing.T) {
			wanted := cfg
			got := mock.Config
			if wanted != got {
				t.Errorf("wanted %#v, got %#v", wanted, got)
			}
		})
	})
}

func TestConsumer_Seek(t *testing.T) {
	// ARRANGE
	offset := &api.Offset{Topic: "topic", Partition: 1, Offset: 42}
	api, mock := ConsumerApi()

	// ACT
	api.Seek(offset, 100)

	// ASSERT
	t.Run("calls Seek", func(t *testing.T) {
		wanted := true
		got := mock.SeekWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with expected offset", func(t *testing.T) {
			wanted := true
			got := mock.SeekOffset == offset
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}

func TestConsumer_Subscribe(t *testing.T) {
	// ARRANGE
	api, mock := ConsumerApi()

	// ACT
	api.Subscribe([]string{"topic.b", "topic.a"}, nil)

	// ASSERT
	t.Run("calls Subscribe", func(t *testing.T) {
		wanted := true
		got := mock.SubscribeWasCalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with expected topics", func(t *testing.T) {
			wanted := []string{"topic.a", "topic.b"}
			got := mock.TopicsSubscribed
			if !set.FromSlice(wanted).Equal(set.FromSlice(got)) {
				t.Errorf("\nwanted %#v\ngot   %#v", wanted, got)
			}
		})
	})
}

func Test_ReadMessage(t *testing.T) {
	t.Run("with messages and an error", func(t *testing.T) {
		// ARRANGE
		m1 := &api.Message{Topic: "1"}
		m2 := &api.Message{Topic: "2"}
		e := errors.New("error")
		msgs := []any{m1, m2, e}

		sut, mock := ConsumerApi()
		mock.Messages(msgs)

		// ACT
		testcases := []struct {
			*api.Message
			error
		}{
			{m1, nil},
			{m2, nil},
			{nil, e},
		}
		for ix, tc := range testcases {
			t.Run(fmt.Sprintf("read %d", ix+1), func(t *testing.T) {
				// ACT
				msg, err := sut.ReadMessage(0)

				// ASSERT
				t.Run("returns message", func(t *testing.T) {
					wanted := tc.Message
					got := msg
					if wanted != got {
						t.Errorf("wanted %v, got %v", wanted, got)
					}
				})

				t.Run("returns error", func(t *testing.T) {
					wanted := tc.error
					got := err
					if wanted != got {
						t.Errorf("wanted %v, got %v", wanted, got)
					}
				})
			})
		}
	})

	t.Run("with messages", func(t *testing.T) {
		// ARRANGE
		m1 := &api.Message{Topic: "1"}
		m2 := &api.Message{Topic: "2"}
		msgs := []any{m1, m2}

		sut, mock := ConsumerApi()
		mock.Messages(msgs)

		// ACT
		testcases := []struct {
			*api.Message
			error
		}{
			{m1, nil},
			{m2, nil},
			{nil, ErrNoMoreMessages},
		}
		for ix, tc := range testcases {
			t.Run(fmt.Sprintf("read %d", ix+1), func(t *testing.T) {
				// ACT
				msg, err := sut.ReadMessage(0)

				// ASSERT
				t.Run("returns message", func(t *testing.T) {
					wanted := tc.Message
					got := msg
					if wanted != got {
						t.Errorf("wanted %v, got %v", wanted, got)
					}
				})

				t.Run("returns error", func(t *testing.T) {
					wanted := tc.error
					got := err
					if wanted != got {
						t.Errorf("wanted %v, got %v", wanted, got)
					}
				})
			})
		}
	})

	t.Run("with message value", func(t *testing.T) {
		// ARRANGE
		m := api.Message{Topic: "value"}
		msgs := []any{m}

		sut, mock := ConsumerApi()
		mock.Messages(msgs)

		// ACT
		msg, _ := sut.ReadMessage(0)

		// ASSERT
		t.Run("returns *message", func(t *testing.T) {
			wanted := true
			got := msg != nil
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("with unsupported type in mocked messages", func(t *testing.T) {
		// ARRANGE
		sut, mock := ConsumerApi()
		mock.messages = []any{42}

		// ACT
		_, err := sut.ReadMessage(0)

		// ASSERT
		t.Run("returns error", func(t *testing.T) {
			wanted := "unsupported type for mocked message"
			got := err.Error()
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})
}
