package confluent

import (
	"testing"

	"github.com/blugnu/kafka/api"
	"github.com/blugnu/kafka/set"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

type mockEvent string

func (m mockEvent) String() string { return string(m) }

func Test_rebalanceEventData(t *testing.T) {

	topic := "topic"
	ptns := []confluent.TopicPartition{
		{Topic: &topic, Partition: 1, Offset: 3},
		{Topic: &topic, Partition: 2, Offset: 2},
		{Topic: &topic, Partition: 3, Offset: 1},
	}
	testcases := []struct {
		name        string
		event       confluent.Event
		wantevent   api.RebalanceEvent
		wantoffsets []api.Offset
		wanterr     error
	}{
		{name: "partitions assigned", event: confluent.AssignedPartitions{Partitions: ptns}, wantevent: api.AssignedPartitions, wantoffsets: []api.Offset{{Topic: "topic", Partition: 1, Offset: 3}, {Topic: "topic", Partition: 2, Offset: 2}, {Topic: "topic", Partition: 3, Offset: 1}}},
		{name: "partitions revoked", event: confluent.RevokedPartitions{Partitions: ptns}, wantevent: api.RevokedPartitions, wantoffsets: []api.Offset{{Topic: "topic", Partition: 1, Offset: 3}, {Topic: "topic", Partition: 2, Offset: 2}, {Topic: "topic", Partition: 3, Offset: 1}}},
		{name: "other", event: mockEvent("some other event"), wanterr: api.UnexpectedRebalanceEventError{Event: "some other event"}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			gotevent, gotoffsets, goterr := parseRebalanceEvent(tc.event)

			// ASSERT
			t.Run("returns error", func(t *testing.T) {
				wanted := tc.wanterr
				got := goterr
				if wanted != got {
					t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
				}
			})

			t.Run("returns event", func(t *testing.T) {
				wanted := tc.wantevent
				got := gotevent
				if wanted != got {
					t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
				}
			})

			t.Run("returns offsets", func(t *testing.T) {
				wanted := tc.wantoffsets
				got := gotoffsets
				if !set.FromSlice(wanted).Equal(set.FromSlice(got)) {
					t.Errorf("\nwanted %+v\n   got %+v", wanted, got)
				}
			})
		})
	}
}
