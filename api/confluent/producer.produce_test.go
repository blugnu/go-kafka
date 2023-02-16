package confluent

import (
	"errors"
	"reflect"
	"testing"

	"github.com/blugnu/kafka/api"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestProducer_Produce(t *testing.T) {
	// ARRANGE
	topic := "topic"
	ptn := int32(1)
	funcerr := errors.New("produce error")

	testcases := []struct {
		name string
		*api.Message
		error
		produces *confluent.Message
	}{
		{name: "fails", Message: &api.Message{}, error: funcerr},
		{name: "no partition specified", Message: &api.Message{Topic: "topic"}, produces: &confluent.Message{TopicPartition: confluent.TopicPartition{Topic: &topic, Partition: confluent.PartitionAny, Offset: confluent.OffsetInvalid}}},
		{name: "partition specified", Message: &api.Message{Topic: "topic", Partition: &ptn}, produces: &confluent.Message{TopicPartition: confluent.TopicPartition{Topic: &topic, Partition: 1, Offset: confluent.OffsetInvalid}}},
		{name: "with headers",
			Message: &api.Message{
				Topic: "topic",
				Headers: map[string][]byte{
					"key": []byte("value"),
				},
			},
			produces: &confluent.Message{
				TopicPartition: confluent.TopicPartition{
					Topic:     &topic,
					Partition: confluent.PartitionAny,
					Offset:    confluent.OffsetInvalid},
				Headers: []confluent.Header{
					{Key: "key", Value: []byte("value")},
				}}},
	}
	for _, tc := range testcases {
		var mp *confluent.Message
		sut := &Producer{
			funcs: &ProducerFuncs{
				Produce: func(m *confluent.Message, c chan confluent.Event) error { mp = m; return tc.error }},
		}
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			err := sut.Produce(tc.Message, nil)

			// ASSERT
			t.Run("returns error", func(t *testing.T) {
				wanted := tc.error
				got := err
				if wanted != got {
					t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
				}
			})

			if err != nil {
				return
			}

			t.Run("produces", func(t *testing.T) {
				wanted := tc.produces
				got := mp
				if !reflect.DeepEqual(wanted, got) {
					t.Errorf("\nwanted %+#v\ngot    %+#v", wanted, got)
				}
			})
		})
	}
}
