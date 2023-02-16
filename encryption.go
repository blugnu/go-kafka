package kafka

type EncryptionHandler interface {
	Decrypt(*Message) error
	Encrypt(*Message) error
}
