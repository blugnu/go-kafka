package kafka

import (
	"github.com/blugnu/kafka/api"
	"github.com/blugnu/logger"
)

type Reader struct {
	EncryptionHandler
	Topics        []string
	Configuration map[string]any
	Log           *logger.Base
	groupId       string
	api           api.Consumer
	log           *logger.Base
	state         readerState
	//	noConnection    bool // used in tests (only)
	//	errors []error
	Api api.Consumer
}

func (r *Reader) getLog() *logger.Log {
	return r.log.Log()
}
