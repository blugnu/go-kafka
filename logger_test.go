package kafka

import (
	"context"

	"github.com/blugnu/logger"
	"github.com/sirupsen/logrus"
)

func InstallTestLogger(ctx context.Context) func() {
	oLog := Log
	log := logrus.New()
	Log = &logger.Base{Context: ctx, Adapter: &logger.LogrusAdapter{Logger: log}}
	return func() { Log = oLog }
}
