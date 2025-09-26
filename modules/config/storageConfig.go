package config

import "time"

type StorageConfig struct {
	Brokers       []string
	ProducerTopic string
	RetryDelays   []time.Duration
}
