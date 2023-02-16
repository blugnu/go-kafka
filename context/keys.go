package context

type contextKey int

const (
	groupIdKey contextKey = iota
	messageReceivedKey
	messageProducedKey
	retryPolicyKey
)
