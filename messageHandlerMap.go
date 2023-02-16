package kafka

type MessageHandlerMap map[string]MessageHandler

func (thm MessageHandlerMap) topicIds() []string {
	ids := make([]string, 0, len(thm))

	// Get all the topics we need to subscribe to (handler topic + any retry topic)
	for topic, handler := range thm {
		ids = append(ids, topic)
		if handler.RetryTopic != "" {
			ids = append(ids, handler.RetryTopic)
		}
	}

	// Replace each topic with the id for that topic
	for i, topic := range ids {
		ids[i], _ = IdForTopic(topic)
	}

	return ids
}
