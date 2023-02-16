package kafka

type readerState int

const (
	rsInitialising readerState = iota
	rsInitialised
	rsConnected
	rsClosed
)

func (s readerState) String() string {
	switch s {
	case rsInitialising:
		return "rsInitialising / <not set>"
	case rsInitialised:
		return "rsInitialised"
	case rsConnected:
		return "rsConnected"
	case rsClosed:
		return "rsClosed"
	default:
		return "<undefined>"
	}
}
