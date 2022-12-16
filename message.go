package kafka

type MessageHeaders map[string]string

type Partition struct {
	Id     int
	Offset int64
}

type Message struct {
	Topic     string
	Partition *Partition
	Headers   MessageHeaders
	Value     []byte
	Error     error
}
