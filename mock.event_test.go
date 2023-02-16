package kafka

type MockEvent string

func (ev MockEvent) Error() string  { return string(ev) }
func (ev MockEvent) String() string { return string(ev) }
