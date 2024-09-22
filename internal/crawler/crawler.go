package crawler

import (
	"minimal-crawler/internal/fetcher"
	"minimal-crawler/internal/parser"
	"minimal-crawler/internal/storage"
)

type Handler struct {
	FetcherService fetcher.FetcherService
	ParserService  parser.ParserService
	StorageService storage.StorageService
}

func (h Handler) Crawler() {
	// logica principal desde la cual se le pasa por referencia el fetcher, parser y storage y se inicializa un crawler con
	// config, fetcher, parser y crawler
	// el crawler es un struct formado por estas 4 cosas el cual cuando se crea un nuevo crawler es lo que se devuelve
}
