package kafka

import (
	"testing"

	"golang.org/x/exp/maps"
)

func Test_MapTopics(t *testing.T) {
	// ARRANGE
	defer func() { idForTopic = TopicMap{}; topicForId = TopicMap{} }()

	// ACT
	MapTopics(TopicMap{"topic.a": "topic-a", "topic.b": "topic-b"})

	// ASSERT
	t.Run("sets idForTopic", func(t *testing.T) {
		wanted := TopicMap{"topic.a": "topic-a", "topic.b": "topic-b"}
		got := idForTopic
		if !maps.Equal(wanted, got) {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("sets topicForId", func(t *testing.T) {
		wanted := TopicMap{"topic-b": "topic.b", "topic-a": "topic.a"}
		got := topicForId
		if !maps.Equal(wanted, got) {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func Test_MapTopic(t *testing.T) {
	// ARRANGE
	defer func() { idForTopic = TopicMap{}; topicForId = TopicMap{} }()

	// ACT
	MapTopic("topic.a", "topic-a")

	// ASSERT
	t.Run("sets idForTopic", func(t *testing.T) {
		wanted := TopicMap{"topic.a": "topic-a"}
		got := idForTopic
		if !maps.Equal(wanted, got) {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})

	t.Run("sets topicForId", func(t *testing.T) {
		wanted := TopicMap{"topic-a": "topic.a"}
		got := topicForId
		if !maps.Equal(wanted, got) {
			t.Errorf("wanted %v, got %v", wanted, got)
		}
	})
}

func Test_Mapping(t *testing.T) {
	// ARRANGE
	defer func() { idForTopic = TopicMap{}; topicForId = TopicMap{} }()

	// ACT
	idForTopic = TopicMap{"topic.name": "topic.id"}
	topicForId = TopicMap{"topic.id": "topic.name"}

	testcases := []struct {
		name     string
		fn       func(string) (string, bool)
		in       string
		isMapped bool
		out      string
	}{
		{name: "mapped topic id", fn: TopicForId, in: "topic.id", isMapped: true, out: "topic.name"},
		{name: "not mapped topic id", fn: TopicForId, in: "not.mapped", isMapped: false, out: "not.mapped"},
		{name: "mapped topic name", fn: IdForTopic, in: "topic.name", isMapped: true, out: "topic.id"},
		{name: "not mapped topic name", fn: IdForTopic, in: "not.mapped", isMapped: false, out: "not.mapped"},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			result, mapped := tc.fn(tc.in)

			// ASSERT
			t.Run("is mapped", func(t *testing.T) {
				wanted := tc.isMapped
				got := mapped
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})

			t.Run("maps to", func(t *testing.T) {
				wanted := tc.out
				got := result
				if wanted != got {
					t.Errorf("wanted %v, got %v", wanted, got)
				}
			})
		})
	}
}
