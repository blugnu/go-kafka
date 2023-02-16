package kafka

type NoEncryption struct{}

func (NoEncryption) Decrypt(msg *Message) error { return nil }
func (NoEncryption) Encrypt(msg *Message) error { return nil }

func (NoEncryption) String() string { return "NoEncryption" }
