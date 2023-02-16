package api

type CommitIntent int

const (
	ReadNext CommitIntent = iota
	ReadAgain
)

func (ci CommitIntent) String() string {
	switch ci {
	case ReadNext:
		return "ReadNext"
	case ReadAgain:
		return "ReadAgain"
	default:
		return "<undefined>"
	}
}
