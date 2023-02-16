package confluent

import confluent "github.com/confluentinc/confluent-kafka-go/kafka"

var newAdminClientFromConsumer = confluent.NewAdminClientFromConsumer

type AdminClient interface {
	Close()
	GetTopicMetadata(string, int) (*confluent.Metadata, error)
}

type adminClientFuncs struct {
	Close       func()
	GetMetadata func(*string, bool, int) (*confluent.Metadata, error)
}

type adminClient struct {
	*confluent.AdminClient
	funcs *adminClientFuncs
}

func NewAdminClientFromConsumer(c *confluent.Consumer) (AdminClient, error) {
	ac, err := newAdminClientFromConsumer(c)
	if err != nil {
		return nil, err
	}

	return &adminClient{
		AdminClient: ac,
		funcs: &adminClientFuncs{
			Close:       ac.Close,
			GetMetadata: ac.GetMetadata,
		},
	}, nil
}
