package config

type StorageConfig struct {
	Brokers       []string
	ProducerTopic string
	ConsumerTopic string
}
