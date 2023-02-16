package kafka

type Headers map[string]string
type RawHeaders map[string][]byte

func (msg *Message) SetHeader(key, value string) {
	if msg.Headers == nil {
		msg.Headers = Headers{}
	}
	if msg.RawHeaders == nil {
		msg.RawHeaders = RawHeaders{}
	}
	msg.Headers[key] = value
	msg.RawHeaders[key] = []byte(value)
}

func (msg *Message) SetRawHeader(key string, value []byte) {
	if msg.Headers == nil {
		msg.Headers = Headers{}
	}
	if msg.RawHeaders == nil {
		msg.RawHeaders = RawHeaders{}
	}
	msg.Headers[key] = string(value)
	msg.RawHeaders[key] = value
}
