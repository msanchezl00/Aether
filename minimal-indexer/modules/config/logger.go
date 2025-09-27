package config

import (
	"os"

	"github.com/sirupsen/logrus"
)

var Logger *logrus.Logger

func InitLogger() {
	Logger = logrus.New()
	Logger.SetLevel(logrus.InfoLevel)
	Logger.SetOutput(os.Stdout)
	Logger.Formatter = &logrus.TextFormatter{
		TimestampFormat: "02-01-2006 15:04:05",
		FullTimestamp:   true,
	}
}
