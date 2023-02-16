package kafka

type producerState int

const (
	psInitialising producerState = iota
	psConnected
	psClosed
)

func (s producerState) String() string {
	switch s {
	case psInitialising:
		return "psInitialising / <not set>"
	case psConnected:
		return "psConnected"
	case psClosed:
		return "psClosed"
	default:
		return "<undefined>"
	}
}
