package confluent

import (
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/set"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestConsumer_Subscribe(t *testing.T) {
	// ARRANGE
	handlerCalled := false

	testcases := []struct {
		name      string
		topics    []string
		rbHandler api.RebalanceEventHandler
		error
		callsFunc     bool
		expectedError error
		expectRbcNil  bool
	}{
		{name: "no topics, no handler, no error", expectedError: api.ErrNoTopics, callsFunc: false},
		{name: "topics, no handler, no error", topics: []string{"topic.a", "topic.b"}, expectRbcNil: true, callsFunc: true},
		{name: "topics + handler, no error", topics: []string{"topic.a", "topic.b"}, rbHandler: func(re api.RebalanceEvent, o []api.Offset) error { handlerCalled = true; return nil }, expectRbcNil: false, callsFunc: true},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			subscribeFuncCalled := false
			subscribedTopics := []string{}
			rbc := (confluent.RebalanceCb)(nil)

			sut := &Consumer{}
			sut.funcs = &ConsumerFuncs{
				SubscribeTopics: func(s []string, rc confluent.RebalanceCb) error {
					subscribeFuncCalled = true
					subscribedTopics = s
					rbc = rc
					return tc.error
				},
			}

			// ACT
			err := sut.Subscribe(tc.topics, tc.rbHandler)

			// ASSERT
			t.Run("returns error", func(t *testing.T) {
				wanted := tc.expectedError
				got := err
				if wanted != got {
					t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
				}
			})

			t.Run("calls funcs.SubscribeTopics()", func(t *testing.T) {
				wanted := tc.callsFunc
				got := subscribeFuncCalled
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}

				if !tc.callsFunc {
					return
				}

				t.Run("with expected topics", func(t *testing.T) {
					wanted := tc.topics
					got := subscribedTopics
					if !set.FromSlice(wanted).Equal(set.FromSlice(got)) {
						t.Errorf("\nwanted %+v\ngot    %+v)", wanted, got)
					}
				})

				t.Run("with nil rbc", func(t *testing.T) {
					wanted := tc.expectRbcNil
					got := rbc == nil
					if wanted != got {
						t.Errorf("wanted %v, got %v", wanted, got)
					}

					if tc.expectRbcNil {
						return
					}

					t.Run("when rbc receives event", func(t *testing.T) {
						// ARRANGE
						handlerCalled = false
						ev := confluent.AssignedPartitions{}

						// ACT
						rbc(nil, ev)

						// ASSERT
						t.Run("rbc calls handler", func(t *testing.T) {
							wanted := true
							got := handlerCalled
							if wanted != got {
								t.Errorf("wanted %v, got %v", wanted, got)
							}
						})
					})

					t.Run("when rbc receives unexpected event", func(t *testing.T) {
						// ARRANGE
						handlerCalled = false
						ev := mockEvent("unexpected")

						// ACT
						rbc(nil, ev)

						// ASSERT
						t.Run("rbc does not call handler", func(t *testing.T) {
							wanted := true
							got := !handlerCalled
							if wanted != got {
								t.Errorf("wanted %v, got %v", wanted, got)
							}
						})
					})
				})
			})
		})
	}
}
