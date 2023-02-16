package confluent

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/logger"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

type callrecord struct {
	adminclient_Close            bool
	adminclient_GetTopicMetadata bool
	assign                       bool
	position                     bool
	unassign                     bool
}

func TestConsumer_Assign(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	acerr := errors.New("admin client error")
	gmerr := errors.New("getmetadata() error")
	assignerr := errors.New("assign() error")
	positionerr := errors.New("position() error")
	unassignerr := errors.New("unassign() error")
	testcases := []struct {
		name                string
		Topics              []string
		UnassignIsCalled    bool
		UnassignReturns     error
		NewAdminClientError error
		GetMetadataError    error
		AssignError         error
		PositionError       error
		result              error
		calls               *callrecord
	}{
		{name: "no topics specified and unassign fails", UnassignReturns: unassignerr, result: unassignerr, calls: &callrecord{unassign: true}},
		{name: "no topics specified and unassign succeeds", UnassignReturns: nil, result: unassignerr, calls: &callrecord{unassign: true}},
		{name: "error creating admin client", Topics: []string{"topic.a", "topic.b"}, NewAdminClientError: acerr, result: acerr, calls: &callrecord{}},
		{name: "error getting metadata", Topics: []string{"topic.a", "topic.b"}, GetMetadataError: gmerr, result: gmerr, calls: &callrecord{adminclient_Close: true, adminclient_GetTopicMetadata: true}},
		{name: "assign fails", Topics: []string{"topic.a", "topic.b"}, AssignError: assignerr, result: assignerr, calls: &callrecord{adminclient_Close: true, adminclient_GetTopicMetadata: true, assign: true}},
		{name: "position fails", Topics: []string{"topic.a", "topic.b"}, PositionError: positionerr, result: positionerr, calls: &callrecord{adminclient_Close: true, adminclient_GetTopicMetadata: true, assign: true, position: true}},
		{name: "successful", Topics: []string{"topic.a", "topic.b"}, calls: &callrecord{adminclient_Close: true, adminclient_GetTopicMetadata: true, assign: true, position: true}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			called := &callrecord{}

			mockac := &MockAdminClient{
				funcs: &adminClientFuncs{},
			}
			mockac.funcs.Close = func() { called.adminclient_Close = true }
			mockac.funcs.GetMetadata = func(topic *string, all bool, ms int) (*confluent.Metadata, error) {
				called.adminclient_GetTopicMetadata = true

				if err := tc.GetMetadataError; err != nil {
					return nil, err
				}
				tmd := map[string]confluent.TopicMetadata{
					*topic: {
						Topic:      *topic,
						Partitions: []confluent.PartitionMetadata{{ID: 1}, {ID: 2}, {ID: 3}},
					},
				}

				return &confluent.Metadata{Topics: tmd}, nil
			}

			sut := &Consumer{
				AdminClient: mockac,
				Log:         &logger.Base{Context: ctx, Adapter: &logger.LogrusAdapter{Logger: logrus.New()}},
				funcs:       &ConsumerFuncs{},
			}
			sut.funcs.Assign = func([]confluent.TopicPartition) error {
				called.assign = true
				return tc.AssignError
			}
			sut.funcs.Position = func(ptns []confluent.TopicPartition) ([]confluent.TopicPartition, error) {
				called.position = true

				tpa := []confluent.TopicPartition{}

				if err := tc.PositionError; err != nil {
					return tpa, err
				}

				for i, p := range ptns {
					tpa = append(tpa, confluent.TopicPartition{
						Topic:     ptns[i].Topic,
						Partition: p.Partition,
						Offset:    0,
					})
				}

				return tpa, nil
			}
			sut.funcs.Unassign = func() error {
				called.unassign = true
				return tc.UnassignReturns
			}

			if err := tc.NewAdminClientError; err != nil {
				onacfn := newAdminClientFromConsumer
				defer func() { newAdminClientFromConsumer = onacfn }()

				newAdminClientFromConsumer = func(*confluent.Consumer) (*confluent.AdminClient, error) {
					return nil, err
				}

				sut.AdminClient = nil
			}

			// ACT
			err := sut.Assign(tc.Topics)

			// ASSERT
			if tc.result == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.result == nil && err != nil {
				t.Run("returns error", func(t *testing.T) {
					wanted := tc.result
					got := err
					if wanted != got {
						t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
					}
				})
			}

			t.Run("calls made", func(t *testing.T) {
				wanted := tc.calls
				got := called
				if *wanted != *got {
					t.Errorf("wanted %#v, got %#v", wanted, got)
				}
			})
		})
	}
}
