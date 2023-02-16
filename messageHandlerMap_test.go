package kafka

import (
	"reflect"
	"testing"

	"github.com/blugnu/kafka/set"
)

func (thm MessageHandlerMap) Equals(other MessageHandlerMap) bool {
	if len(thm) != len(other) {
		return false
	}

	for k, v := range thm {
		o := other[k]

		if o.RetryTopic != v.RetryTopic ||
			o.RetryPolicy != v.RetryPolicy ||
			o.DeferralHandler != v.DeferralHandler {
			return false
		}

		ofp := reflect.ValueOf(o.Func).Pointer()
		vfp := reflect.ValueOf(v.Func).Pointer()
		if ofp != vfp {
			return false
		}
	}

	return true
}

func TestMessageHandlerMap(t *testing.T) {
	// ARRANGE
	sut := MessageHandlerMap{
		"topica": MessageHandler{
			RetryTopic: "topica_retry",
		},
		"topicb": MessageHandler{
			RetryTopic: "topicb_retry",
		},
		"topicc": MessageHandler{},
	}

	defer func() { idForTopic = TopicMap{}; topicForId = TopicMap{} }()
	idForTopic = TopicMap{"topicc": "c"}
	topicForId = TopicMap{"c": "topicc"}

	t.Run("topicIds", func(t *testing.T) {
		// ACT
		ids := sut.topicIds()

		// ASSERT
		t.Run("result", func(t *testing.T) {
			wanted := []string{"topica", "topica_retry", "topicb", "topicb_retry", "c"}
			got := ids
			if !set.FromSlice(wanted).Equal(set.FromSlice(got)) {
				t.Errorf("\nwanted %v\ngot    %v", wanted, got)
			}
		})
	})
}
