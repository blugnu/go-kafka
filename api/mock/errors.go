package mock

type Error string

func (e Error) Error() string { return string(e) }

const ErrNoMoreMessages = Error("no more messages")
