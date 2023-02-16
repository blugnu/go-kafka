package kafka

import (
	"bytes"

	"golang.org/x/exp/maps"
)

func (msg *Message) Equal(other *Message) bool {
	if msg == nil {
		return other == nil
	}

	if sameOffset := msg.Topic == other.Topic &&
		((msg.Partition == nil && other.Partition == nil) || ((msg.Partition != nil && other.Partition != nil) && (*msg.Partition == *other.Partition))) &&
		((msg.Offset == nil && other.Offset == nil) || ((msg.Offset != nil && other.Offset != nil) && (*msg.Offset == *other.Offset))); !sameOffset {
		return false
	}

	if sameKey := bytes.Equal(msg.Key, other.Key); !sameKey {
		return false
	}

	if sameValue := bytes.Equal(msg.Value, other.Value); !sameValue {
		return false
	}

	if sameTimestamp := (msg.Timestamp == nil) && (other.Timestamp == nil) ||
		((msg.Timestamp != nil && other.Timestamp != nil) && (*msg.Timestamp == *other.Timestamp)); !sameTimestamp {
		return false
	}

	if sameHeaders := maps.Equal(msg.Headers, other.Headers); !sameHeaders {
		return false
	}

	if len(msg.RawHeaders) != len(other.RawHeaders) {
		return false
	}
	keys := []string{}
	for k := range msg.RawHeaders {
		keys = append(keys, k)
	}
	for _, k := range keys {
		if !bytes.Equal(msg.RawHeaders[k], other.RawHeaders[k]) {
			return false
		}
	}

	return true
}
