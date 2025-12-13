package config

type CrawlerConfig struct {
	Seeds       []map[string]int
	Recursive   bool
	Timeout     float32
	Workers     int32
	Robots      bool
	ExpiryHours int32
}
