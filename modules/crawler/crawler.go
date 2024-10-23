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

	defer config.Logger.Info("crawler finished successfully")
	// cada seed se encargara de una url(parsear, storage ...) sera el padre de esta
	// y cuando investigue en profundidad por cada nueva url se crearan nuevas goroutines de forma recursiva
	// por cada nivel de profundidad una nueva generacion de goroutines se creara hasta llegar  aun limite
	// al final se podra observar un arbol de goroutines cada una encargada de una url nueva
	// con url nueva se refiere a un nuevo dominio o subdominio
	// por lo que en anchura se encargara la misma goroutine pero en profundidad se encargaran otras goroutines
	// generadas de forma dinamica

	// se recorre la lista de seeds que van a aser crawleadas
	for _, seedMap := range h.CrawlerConfing.Seeds {
		for url := range seedMap {
			// se agrega al grupo de goroutines
			wg.Add(1)
			// funcion anonima para lanzar una url por cada goroutine
			go func(url string) {
				// se espera a q acabe la funcion anonima para liberarse
				defer wg.Done()

				htmlUTF8, err := h.FetcherService.Fetch(url)
				if err != nil {
					config.Logger.Errorf("Error fetching url: %v", err)
				}

				_, err = h.ParserService.Parse(htmlUTF8)
				if err != nil {
					config.Logger.Errorf("Error parsing url: %v", err)
				}

			}(url)
		}
	}

	config.Logger.Info("crawler started successfully")
	// proceso padre espera a que las goroutines mueran
	wg.Wait()
}
