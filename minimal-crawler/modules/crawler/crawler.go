package crawler

import (
	"context"
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
var pool chan struct{}
var wg sync.WaitGroup
var mu sync.Mutex
var crawledDomains []string
var notCrawledDomains []string

func (h *Handler) InitCrawler(ctx context.Context) {
	// los defer se resuelven el LIFO
	defer config.Logger.Info("crawler finished successfully")
	// proceso padre espera a que las goroutines mueran
	defer wg.Wait()

	pool = make(chan struct{}, h.CrawlerConfing.Workers)
	config.Logger.Info("crawler started successfully")

	wg.Add(1)
	go func() {
		defer wg.Done()
		// se recorre la lista de seeds que van a aser crawleadas
		for _, seedMap := range h.CrawlerConfing.Seeds {
			for rawURL, deep := range seedMap {
				// se agrega al grupo de goroutines
				wg.Add(1)
				pool <- struct{}{}

				var crawledInternalURLs []string
				if h.CrawlerConfing.Robots {
					_, disallows, err := utils.GetRobotsRules(rawURL)
					if err != nil {
						config.Logger.Errorf("Error getting robots.txt rules for %s: %v", rawURL, err)
					} else {
						crawledInternalURLs = append(crawledInternalURLs, disallows...)
					}
				}
				// funcion anonima para lanzar una url por cada goroutine
				go h.Crawler(rawURL, &crawledInternalURLs, false, deep)
			}
		}
	}()
}

func (h *Handler) Crawler(rawURL string, crawledInternalURLs *[]string, isInternalURL bool, deep int) {
	// si la url es interna se hace una cosa u otra
	if !isInternalURL {
		// liberamos espacio en el pool
		defer func() { <-pool }()
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
		flag := appendAndVerifyCrawledDomain(domain)
		if !flag {
			return
		}
		// si de verdad el dominio no ha sido crawleado, pasara por aqui por eso lo escribo aqui
		defer config.Logger.Infof("Finished crwled domain: %v", rawURL)
	}

	// agregar la url que se va a crawlear a la lista de urls internas crawleadas
	flag := appendAndVerifyInternalURL(crawledInternalURLs, rawURL)
	if !flag {
		return
	}

	htmlUTF8, err := h.FetcherService.Fetch(rawURL, h.CrawlerConfing.Timeout)
	if err != nil {
		config.Logger.Errorf("Error fetching url: %s Error: %v", rawURL, err)
		return
	}

	parsedData, err := h.ParserService.Parse(htmlUTF8)
	if err != nil {
		config.Logger.Errorf("Error parsing url: %s Error: %v", rawURL, err)
		return
	}

	err = h.StorageService.KafkaStorage(utils.BuildPayload(rawURL, parsedData), 0)
	if err != nil {
		config.Logger.Errorf("Error storing url: %s Error: %v", rawURL, err)
		return
	}

	// extraemos los dominios descubiertos y extraemos los que no hemos investigado
	freeExternalURLs, freeInternalURLs, err := utils.VerifyDomainsAndInternal(crawledDomains, utils.ExtractExternalURLs(parsedData), *crawledInternalURLs, utils.ExtractInternalURLs(parsedData, rawURL), rawURL)
	if err != nil {
		config.Logger.Errorf("Error verifying domains: %v", err)
		return
	}

	// appendea los dominios libres encontrados a la lista de dominios no crawleados
	appendAndVerifyNotCrawledDomains(freeExternalURLs)

	if deep >= 0 {
		mu.Lock()
		for _, notCrawledDomain := range notCrawledDomains {
			select {
			case pool <- struct{}{}: // Intenta enviar un valor al canal
				// se agrega al grupo de goroutines
				wg.Add(1)

				var crawledInternalURLs []string
				if h.CrawlerConfing.Robots {
					_, disallows, err := utils.GetRobotsRules(rawURL)
					if err != nil {
						config.Logger.Errorf("Error getting robots.txt rules for %s: %v", rawURL, err)
					} else {
						crawledInternalURLs = append(crawledInternalURLs, disallows...)
					}
				}
				// lanza una gorotine para crawlear un dominio nuevo
				go h.Crawler(notCrawledDomain, &crawledInternalURLs, false, deep-1)
			default:
			}
		}
		mu.Unlock()
	}

	// crawling en horizontal por la misma goroutine
	for _, freeInternalURL := range freeInternalURLs {
		h.Crawler(freeInternalURL, crawledInternalURLs, true, deep)
	}
}

func appendAndVerifyCrawledDomain(domain string) bool {
	mu.Lock()
	defer mu.Unlock()

	notCrawledDomains = utils.RemoveDomain(notCrawledDomains, domain)

	for _, crawledDomain := range crawledDomains {
		if crawledDomain == domain {
			return false
		}
	}

	crawledDomains = append(crawledDomains, domain)
	return true
}

func appendAndVerifyNotCrawledDomains(notCrawledURLsAux []string) {
	mu.Lock()
	defer mu.Unlock()

	notCrawledMap := make(map[string]struct{})
	for _, domain := range notCrawledDomains {
		notCrawledMap[domain] = struct{}{}
	}

	CrawledMap := make(map[string]struct{})
	for _, domain := range crawledDomains {
		CrawledMap[domain] = struct{}{}
	}

	for _, notCrawledURLAux := range notCrawledURLsAux {
		notCrawledDomainAux, _ := utils.ExtractDomain(notCrawledURLAux)
		if _, exists := notCrawledMap[notCrawledURLAux]; !exists {
			if _, exists := CrawledMap[notCrawledDomainAux]; !exists {
				notCrawledDomains = append(notCrawledDomains, notCrawledURLAux)
			}
		}
	}
}

func appendAndVerifyInternalURL(crawledInternalURLs *[]string, internalURL string) bool {
	for _, crawledInternalURL := range *crawledInternalURLs {
		if crawledInternalURL == internalURL {
			return false
		}
	}
	*crawledInternalURLs = append(*crawledInternalURLs, internalURL)
	return true
}

// add a log to count howmuch is discover in horiontal on this domain
//config.Logger.Infof(rawURL+"--> %d", len(*crawledInternalURLs))

/* 	// para visualizar el json formateado para hacer pruebas
   	dataByte, err := json.MarshalIndent(parsedData, "", "  ")
   	if err != nil {
   		return
   	}

   	domain, err := utils.ExtractDomain(rawURL)
   	if err != nil {
   		config.Logger.Errorf("Error extracting domain: %v", err)
   		return
   	}
   	// Abrir el archivo en modo append (agregar)
   	file, err := os.OpenFile("data/"+domain+".txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
   	if err != nil {
   		return
   	}

   	_, err = file.Write([]byte("-------------------------------------------------" + rawURL + "-------------------------------------------------\n"))
   	if err != nil {
   		return
   	}
   	_, err = file.Write(dataByte)
   	if err != nil {
   		return
   	}
   	_, err = file.Write([]byte("--------------------------------------------------------------------------------------------------\n"))
   	if err != nil {
   		return
   	}
   	file.Close() */
