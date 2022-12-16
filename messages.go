package kafka

func NewMessage(t string, v interface{}) *Message {
	return &Message{
		Topic: t,
		Value: v.([]byte),
	}
}

// FIXME: Move to _test
func StringMessage(t string, v string) *Message {
	return &Message{
		Topic: t,
		Value: []byte(v),
	}
}
