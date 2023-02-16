package kafka

type consumerState int

const (
	csInitialising consumerState = iota

	csRunning          // when the consumer is running
	csStopped          // when the consumer has stopped normally
	csClosed           // when the consumer has been closed
	csInitialiseFailed // when c.initialise() has failed
	csConnectFailed    // when c.connect() has failed
	csSubscribeFailed  // when c.subscribe() has failed
	csStoppedWithError // when the consumer has stopped due to an error
	csCloseFailed      // when c.close() has failed
)

func (s consumerState) String() string {
	switch s {
	case csInitialising:
		return "csInitialising / <not set>"
	case csRunning:
		return "csRunning"
	case csStopped:
		return "csStopped"
	case csInitialiseFailed:
		return "csInitialiseFailed"
	case csConnectFailed:
		return "csConnectFailed"
	case csSubscribeFailed:
		return "csSubscribeFailed"
	case csStoppedWithError:
		return "csStoppedWithError"
	case csClosed:
		return "csClosed"
	case csCloseFailed:
		return "csCloseFailed"
	default:
		return "<undefined>"
	}
}
