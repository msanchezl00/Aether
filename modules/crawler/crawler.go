package crawler

import (
	config "minimal-crawler/modules/config"
	"minimal-crawler/modules/fetcher"
	"minimal-crawler/modules/parser"
	"minimal-crawler/modules/storage"
	"sync"
)

type Handler struct {
	CrawlerConfing config.CrawlerConfig
	FetcherService fetcher.FetcherInterface
	ParserService  parser.ParserInterface
	StorageService storage.StorageInterface
}

// variable para tener controlados los procesos y esperar a su muerte de forma ordenada
var wg sync.WaitGroup

func (h *Handler) InitCrawler() {

	// cada seed se encargara de una url(parsear, storage ...) sera el padre de esta
	// y cuando investigue en profundidad por cada nueva url se crearan nuevas goroutines de forma recursiva
	// por cada nivel de profundidad una nueva generacion de goroutines se creara hasta llegar  aun limite
	// al final se podra observar un arbol de goroutines cada una encargada de una url nueva
	// con url nueva se refiere a un nuevo dominio o subdominio
	// por lo que en anchura se encargara la misma goroutine pero en profundidad se encargaran otras goroutines
	// generadas de forma dinamica

	// se recorre la lista de seeds que van a aser crawleadas
	for i, seedMap := range h.CrawlerConfing.Seeds {
		for url := range seedMap {
			// funcion anonima para lanzar una url por cada goroutine
			// se agrega al grupo de goroutines
			wg.Add(1)
			go func(url string) {
				// se espera a q acabe la funcion anonima para liberarse
				defer wg.Done()
				htmlString, err := h.FetcherService.Fetch(url)
				if err != nil {
					config.Logger.Errorf("Error fetching url: %v", err)
				}
				config.Logger.Infof("url: %d %s", i, htmlString)
			}(url)
		}
	}

	// proceso padre espera a que las goroutines mueran
	wg.Wait()
}
