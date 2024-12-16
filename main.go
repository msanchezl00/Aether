package main

import (
	"encoding/json"
	"io"
	config "minimal-crawler/modules/config"
	crawler "minimal-crawler/modules/crawler"
	fetcher "minimal-crawler/modules/fetcher"
	parser "minimal-crawler/modules/parser"
	storage "minimal-crawler/modules/storage"
	"os"
)

// estructura de la configuracion del json
var Config struct {
	Seeds     []map[string]int `json:"seeds"`
	Robots    bool             `json:"robots"`
	Recursive bool             `json:"recursive"`
	Data      struct {
		Metadata bool `json:"metadata"`
		Links    bool `json:"links"`
		Text     bool `json:"text"`
		Images   bool `json:"images"`
	} `json:"data"`
	Indexers []string `json:"indexers"`
	Timeout  float32  `json:"timeout"`
}

func main() {

	// inicializacion de la configuracion del logger
	config.InitLogger()

	// abrimos el archivo y generamos un tipo File de go
	file, err := os.Open("config.json")
	if err != nil {
		config.Logger.Errorf("Error opening config file: %v", err)
		return
	}
	defer file.Close()

	// guardamos el File en un array de bytes
	fileByte, err := io.ReadAll(file)
	if err != nil {
		config.Logger.Errorf("Error saving config file: %v", err)
		return
	}

	// deserializamos el json del array de bytes y lo guardamos en
	// la variable general Config
	err = json.Unmarshal(fileByte, &Config)
	if err != nil {
		config.Logger.Errorf("Error unmarshaling config file: %v", err)
		return
	}

	// uso de ampersan para pasarle la referencia del objeto y poder usar
	// la interfaz, de esta manera solo tienes acceso a las funciones definidas en la interfaz
	// tambien permite modificar el objeto original y no perder la informacion
	// en tiempo de ejecucion
	fetcher := &fetcher.Service{
		FetcherConfig: config.FetcherConfig{
			Robots: Config.Robots,
		},
	}

	// creacion del parser
	parser := &parser.Service{
		ParserConfig: config.ParserConfig{
			Metadata: Config.Data.Metadata,
			Links:    Config.Data.Links,
			Text:     Config.Data.Text,
			Images:   Config.Data.Images,
		},
	}

	// creacion del storage
	storage := &storage.Service{
		StorageConfig: config.StorageConfig{
			Indexers: Config.Indexers,
		},
	}

	// Creacion del crawler con las 3 partes fundamentales ya configuradas
	// y creacion de la configuracion general del crawler destinada a
	// control de flujo de la aplicacion
	crawler := &crawler.Handler{
		CrawlerConfing: config.CrawlerConfig{
			Seeds:     Config.Seeds,
			Recursive: Config.Recursive,
			Timeout:   Config.Timeout,
		},
		FetcherService: fetcher,
		ParserService:  parser,
		StorageService: storage,
	}

	// iniciar crawler, validara los storage proporcionados(que esten disponibles)
	// y validara que las url base de las seeds(que esten activas[si devuelven algun codigo de error se borraran de las seeds])
	crawler.InitCrawler()
}
