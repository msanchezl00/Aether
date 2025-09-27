package config

import "time"

type StorageConfig struct {
	Brokers       []string
	ProducerTopic string
	ConsumerTopic string
	RetryDelays   []time.Duration
}
