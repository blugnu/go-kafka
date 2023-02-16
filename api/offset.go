package api

import "fmt"

type Offset struct {
	Topic     string
	Partition int32
	Offset    int64
}

func (o *Offset) String() string {
	return fmt.Sprintf("%s,%d:%d", o.Topic, o.Partition, o.Offset)
}
