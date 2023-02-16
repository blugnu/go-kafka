package confluent

import (
	"errors"
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestAdminClient_GetTopicMetadata(t *testing.T) {
	// ARRANGE
	fncalled := false
	fnerr := errors.New("func error")
	topicSpecified := ""
	allTopicsSpecified := false
	timeoutSpecified := -1

	sut := &adminClient{
		funcs: &adminClientFuncs{
			GetMetadata: func(s *string, b bool, i int) (*confluent.Metadata, error) {
				fncalled = true
				topicSpecified = *s
				allTopicsSpecified = b
				timeoutSpecified = i
				return nil, fnerr
			},
		},
	}

	// ACT
	_, err := sut.GetTopicMetadata("topic", 100)

	// ASSERT
	t.Run("calls funcs.GetMetadata", func(t *testing.T) {
		wanted := true
		got := fncalled
		if wanted != got {
			t.Errorf("wanted %v, got %v", wanted, got)
		}

		t.Run("with topic", func(t *testing.T) {
			wanted := "topic"
			got := topicSpecified
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("with allTopics", func(t *testing.T) {
			wanted := false
			got := allTopicsSpecified
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})

		t.Run("with timeout", func(t *testing.T) {
			wanted := 100
			got := timeoutSpecified
			if wanted != got {
				t.Errorf("wanted %v, got %v", wanted, got)
			}
		})
	})

	t.Run("returns error", func(t *testing.T) {
		wanted := fnerr
		got := err
		if wanted != got {
			t.Errorf("wanted %[1]T (%[1]q), got %[2]T (%[2]q)", wanted, got)
		}
	})
}
