package kafka

type TopicMap map[string]string

var idForTopic = TopicMap{}
var topicForId = TopicMap{}

func IdForTopic(t string) (id string, mapped bool) {
	if id, mapped = idForTopic[t]; mapped {
		return
	}
	return t, false
}

func TopicForId(id string) (topic string, mapped bool) {
	if topic, mapped = topicForId[id]; mapped {
		return
	}
	return id, false
}

func MapTopic(log string, id string) {
	idForTopic[log] = id
	topicForId[id] = log
}

func MapTopics(tm TopicMap) {
	for log, id := range tm {
		idForTopic[log] = id
		topicForId[id] = log
	}
}
