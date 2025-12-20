package config

import (
	"time"

	"golang.org/x/time/rate"
)

type StorageConfig struct {
	Brokers       []string
	ProducerTopic string
	MaxBytes      int
	RetryDelays   []time.Duration
	Limiter       *rate.Limiter
}
