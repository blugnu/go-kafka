package kafka

import (
	"github.com/blugnu/kafka/context"
	"github.com/blugnu/logger"
)

func init() {
	logger.WithEnrichment(enrichLog)
}

type logInfo struct {
	ConsumerGroupId string                  `json:"consumerGroupId,omitempty"`
	RetryPolicy     string                  `json:"retryPolicy,omitempty"`
	MessageReceived *context.MessageSummary `json:"messageReceived,omitempty"`
	MessageProduced *context.MessageSummary `json:"messageProduced,omitempty"`
}

func enrichLog(ctx context.Context, log *logger.Log) *logger.Log {
	info := &logInfo{}

	if groupId, isSet := context.GroupId(ctx); isSet {
		info.ConsumerGroupId = groupId
	}
	if policy := context.RetryPolicy(ctx); policy != "" {
		info.RetryPolicy = policy
	}
	if msg := context.MessageReceived(ctx); msg != nil {
		info.MessageReceived = msg
	}
	if msg := context.MessageProduced(ctx); msg != nil {
		info.MessageProduced = msg
	}

	log = log.WithField("kafka", info)
	return log
}

func getBaseLogger(base *logger.Base, ctx context.Context) *logger.Base {
	if base != nil {
		return base.WithContext(ctx)
	}
	return Log.WithContext(ctx)
}
