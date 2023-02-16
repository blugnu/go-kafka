package kafka

type encryptionHandlerRecorder struct {
	wasCalled bool
}

type MockEncryptionHandler struct {
	decrypt      encryptionHandlerRecorder
	encrypt      encryptionHandlerRecorder
	decryptError error
	encryptError error
}

func (h *MockEncryptionHandler) Decrypt(*Message) error {
	h.decrypt.wasCalled = true
	return h.decryptError
}
func (h *MockEncryptionHandler) Encrypt(*Message) error {
	h.encrypt.wasCalled = true
	return h.encryptError
}
