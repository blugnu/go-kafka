package kafka

// NewCopy returns a copy of the receiver message, initialised to be produced
// as a new message.  In the returned message, Headers, Key and Value are
// preserved; Topic, Partition, Offset and Timestamp fields are reset to zero
// values.  As a result, the Topic of the returned message must be set before
// the message is produced.
//
// Similarly any deferral header is removed.
//
// This is intended to avoid errors being introduced by creating a copy of a
// message intended to be produced to a different topic but then forgetting to
// set the intended destination topic or perpetually deferring a message by
// neglecting to set an adjusted deferral header.
func (m *Message) NewCopy() *Message {
	msg := &Message{
		Key:        m.Key,
		Value:      m.Value,
		Headers:    Headers{},
		RawHeaders: RawHeaders{},
	}

	for k, v := range m.RawHeaders {
		msg.Headers[k] = string(v)
		msg.RawHeaders[k] = v
	}

	delete(msg.Headers, "blugnu/kafka:deferred")
	delete(msg.RawHeaders, "blugnu/kafka:deferred")

	return msg
}
