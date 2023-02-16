package confluent

import confluent "github.com/confluentinc/confluent-kafka-go/kafka"

func (c *Consumer) Assign(topics []string) error {
	log := c.Log.Log()

	if topics == nil {
		if err := c.funcs.Unassign(); err != nil {
			return err
		}
		log.Debug("partitions unassigned")
		return nil
	}

	ac := c.AdminClient
	if ac == nil {
		var err error
		ac, err = NewAdminClientFromConsumer(c.Consumer)
		if err != nil {
			return err
		}
	}
	defer ac.Close()

	offsets := []confluent.TopicPartition{}

	for _, t := range topics {
		md, err := ac.GetTopicMetadata(t, 5000)
		if err != nil {
			return err
		}
		log.Debugf("obtained metadata for topic %s (%d partitions)", t, len(md.Topics[t].Partitions))

		for _, p := range md.Topics[t].Partitions {
			offsets = append(offsets, confluent.TopicPartition{
				Topic:     &t,
				Partition: p.ID,
				Offset:    confluent.OffsetBeginning,
			})
		}
	}

	// TODO: Accept a time.Time; if non-zero then use c.funcs.OffsetsForTime to
	// determine an initial offset for each partition corresponding to the earliest
	// message after that time (i.e. to support: Reader.ReadFrom(time))

	if err := c.funcs.Assign(offsets); err != nil {
		return err
	}

	pns, err := c.funcs.Position(offsets)
	if err != nil {
		return err
	}
	for _, pos := range pns {
		log.Debugf("positioned on %s@%d:%s", *pos.Topic, pos.Partition, pos.Offset)
	}

	log.Debug("partitions assigned")
	
	return nil
}
