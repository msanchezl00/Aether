package config

type ConsumerConfig struct {
	Brokers       []string
	ConsumerTopic string
	GroupID       string
	MinBytes      int
	MaxBytes      int
}
