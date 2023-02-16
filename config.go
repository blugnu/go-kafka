package kafka

import (
	"github.com/blugnu/logger"
)

var BootstrapServers []string
var Deferral DeferralHandler
var Encryption EncryptionHandler = &NoEncryption{}
var Delivery ProducerMode = IdempotentDelivery
var Log *logger.Base
var DefaultProducer MessageProducer
