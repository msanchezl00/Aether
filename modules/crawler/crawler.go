package crawler

import (
	config "minimal-crawler/modules/config"
	"minimal-crawler/modules/fetcher"
	"minimal-crawler/modules/parser"
	"minimal-crawler/modules/storage"
	"minimal-crawler/utils"
	"sync"
)

type Handler struct {
	CrawlerConfing config.CrawlerConfig
	FetcherService fetcher.FetcherInterface
	ParserService  parser.ParserInterface
	StorageService storage.StorageInterface
}

// variable para tener controlados los subprocesos por parte del padre maestro y esperar a su muerte de forma ordenada
var wg sync.WaitGroup
var mu sync.Mutex
var crawledDomains []string

func (h *Handler) InitCrawler() {
	// los defer se resuelven el LIFO
	defer config.Logger.Info("crawler finished successfully")
	// proceso padre espera a que las goroutines mueran
	defer wg.Wait()

	config.Logger.Info("crawler started successfully")

	// se recorre la lista de seeds que van a aser crawleadas
	for _, seedMap := range h.CrawlerConfing.Seeds {
		for rawURL, deep := range seedMap {
			// se agrega al grupo de goroutines
			wg.Add(1)
			// funcion anonima para lanzar una url por cada goroutine
			go h.Crawler(rawURL, deep)
		}
	}
}

func (h *Handler) Crawler(rawURL string, deep int) {
	// hace el done despues del wait para esperar primero a que acaben sus hijos
	defer wg.Done()

	// comprueba que la profundidad no sea menor a 0, si es asi simplemente acaba la funcion
	// y se rompe la cadena de recursividad
	if deep < 0 {
		return
	}

	domain, err := utils.ExtractDomain(rawURL)
	if err != nil {
		config.Logger.Errorf("Error extracting domain: %v", err)
		return
	}

	// agregar la url que se va a crawlear a la lista de dominios crawleados
	flag := appendAndVerifyDomain(domain)
	if !flag {
		return
	}

	// restamos 1 a la profundidad
	deep -= 1

	htmlUTF8, err := h.FetcherService.Fetch(rawURL, h.CrawlerConfing.Timeout)
	if err != nil {
		config.Logger.Errorf("Error fetching url: %v", err)
	}

	parsedData, err := h.ParserService.Parse(htmlUTF8)
	if err != nil {
		config.Logger.Errorf("Error parsing url: %v", err)
	}

	// extraemos los dominios descubiertos y extraemos los que no hemos investigado
	freeURLs, err := utils.VerifyDomains(crawledDomains, utils.ExtractURLs(parsedData))
	if err != nil {
		config.Logger.Errorf("Error verifying domains: %v", err)
	}

	// para visualizar el json formateado para hacer pruebas
	/* 	dataByte, err := json.MarshalIndent(parsedData, "", "  ")
	   	if err != nil {
	   		return
	   	}
	   	config.Logger.Infof(string(dataByte)) */
	/* 	dataByte, err := json.Marshal(data)
	   	if err != nil {
	   		return nil, err
	   	} */

	for _, val := range crawledDomains {
		config.Logger.Infof(val)
	}

	for _, freeURL := range freeURLs {
		// se agrega al grupo de goroutines
		wg.Add(1)
		// funcion anonima para lanzar una url por cada dominio libre
		go h.Crawler(freeURL, deep)
	}
	// obtener los links que apuntan a dominios externos para
	// empezar la recursividad y hacer un for recorriendolos y mandando
	// a un subproceso por cada link de dominio externo al principal
	// de la url que entra por parametro en esta funcion
	// al hacer la llamada recursiva es muy importante restarle 1 a deep
	// para que haya opcion a salida y que deep llegue a -1, es la condicion
	// de salida de la recursividad

	// storage al kafka-->consumidores-->namenode(nodo de entrada en hadoop)
}

func appendAndVerifyDomain(domain string) bool {
	mu.Lock()
	defer mu.Unlock()
	for _, crawledDomain := range crawledDomains {
		if crawledDomain == domain {
			return false
		}
	}
	crawledDomains = append(crawledDomains, domain)
	return true
}
