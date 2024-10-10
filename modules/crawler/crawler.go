package crawler

import (
	core "minimal-crawler/modules/config"
	"minimal-crawler/modules/fetcher"
	"minimal-crawler/modules/parser"
	"minimal-crawler/modules/storage"
)

type Handler struct {
	CrawlerConfing core.CrawlerConfig
	FetcherService fetcher.FetcherInterface
	ParserService  parser.ParserInterface
	StorageService storage.StorageInterface
}

func (h *Handler) InitCrawler() {
	// logica principal desde la cual se le pasa por referencia el fetcher, parser y storage y se inicializa un crawler con
	// config, fetcher, parser y crawler
	// el crawler es un struct formado por estas 4 cosas el cual cuando se crea un nuevo crawler es lo que se devuelve
}
