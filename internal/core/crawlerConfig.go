package core

type CrawlerConfig struct {
	Seeds     []map[string]int
	Recursive bool
	Robots    bool
}
