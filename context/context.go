package context

import (
	"context"
)

type Context = context.Context

func GroupId(ctx context.Context) (string, bool) {
	if v := ctx.Value(groupIdKey); v != nil {
		return v.(string), true
	}
	return "", false
}

func MessageProduced(ctx context.Context) *MessageSummary {
	if v := ctx.Value(messageProducedKey); v != nil {
		return v.(*MessageSummary)
	}
	return nil
}

func MessageReceived(ctx context.Context) *MessageSummary {
	if v := ctx.Value(messageReceivedKey); v != nil {
		return v.(*MessageSummary)
	}
	return nil
}

func RetryPolicy(ctx context.Context) string {
	if v := ctx.Value(retryPolicyKey); v != nil {
		return v.(string)
	}
	return ""
}

func WithGroupId(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, groupIdKey, id)
}

func WithMessageProduced(ctx context.Context, msg *MessageSummary) context.Context {
	return context.WithValue(ctx, messageProducedKey, msg)
}

func WithMessageReceived(ctx context.Context, msg *MessageSummary) context.Context {
	return context.WithValue(ctx, messageReceivedKey, msg)
}

func WithRetryPolicy(ctx context.Context, policy string) context.Context {
	return context.WithValue(ctx, retryPolicyKey, policy)
}
