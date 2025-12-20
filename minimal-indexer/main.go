package main

import (
	"context"
	"encoding/json"
	"io"
	config "minimal-indexer/modules/config"
	consumer "minimal-indexer/modules/consumer"
	"minimal-indexer/modules/indexer"
	storage "minimal-indexer/modules/storage"
	"minimal-indexer/modules/transformer"
	"os"
	"time"
)

// estructura de la configuracion del json
var Config struct {
	Brokers       []string `json:"brokers"`
	ProducerTopic string   `json:"producer-topic"`
	ConsumerTopic string   `json:"consumer-topic"`
	RetryDelays   []int    `json:"retry-delays"`
	GroupID       string   `json:"group-id"`
	MinBytes      int      `json:"min-bytes"`
	MaxBytes      int      `json:"max-bytes"`
	Workers       int      `json:"workers"`
}

// estructura de la configuracion del json
var Datasets struct {
	Keywords map[string][]string `json:"keywords"`
	Patterns []string            `json:"patterns"`
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

	// deserializamos el json del array de bytes y lo guardamos en
	// la variable general Config
	err = json.Unmarshal(fileByte, &Config)
	if err != nil {
		config.Logger.Errorf("Error unmarshaling config file: %v", err)
		return
	}
	b, err := json.MarshalIndent(Config, "", "  ")
	if err != nil {
		config.Logger.Errorf("Error al serializar config: %v", err)
		return
	}
	config.Logger.Infof("Configuración cargada:\n%s", string(b))

	// sobreescribimos la configuracion de brokers si existe la variable de entorno
	if brokersEnv := os.Getenv("CONF_BROKERS"); brokersEnv != "" {
		var brokers []string
		if err := json.Unmarshal([]byte(brokersEnv), &brokers); err == nil {
			Config.Brokers = brokers
		}
	}

	// sobreescribimos la configuracion del topic del productor si existe la variable de entorno
	if topic := os.Getenv("CONF_PRODUCER_TOPIC"); topic != "" {
		Config.ProducerTopic = topic
	}

	// sobreescribimos la configuracion del topic del productor si existe la variable de entorno
	if topic := os.Getenv("CONF_CONSUMER_TOPIC"); topic != "" {
		Config.ConsumerTopic = topic
	}

	// abrimos el archivo y generamos un tipo File de go
	file, err = os.Open("dataset.json")
	if err != nil {
		config.Logger.Errorf("Error opening config file: %v", err)
		return
	}
	defer file.Close()

	// guardamos el File en un array de bytes
	fileByte, err = io.ReadAll(file)
	if err != nil {
		config.Logger.Errorf("Error saving config file: %v", err)
		return
	}

	// deserializamos el json del array de bytes y lo guardamos en
	// la variable general Config
	err = json.Unmarshal(fileByte, &Datasets)
	if err != nil {
		config.Logger.Errorf("Error unmarshaling config file: %v", err)
		return
	}

	var retryDurations []time.Duration
	for _, ms := range Config.RetryDelays {
		retryDurations = append(retryDurations, time.Duration(ms)*time.Millisecond)
	}

	// creacion del consumer, se va a dedicar a consumir los eventos kafka y pasarlos al indexer de forma asincrona mediante goroutines
	consumer := &consumer.Service{
		ConsumerConfig: config.ConsumerConfig{
			Brokers:       Config.Brokers,
			ConsumerTopic: Config.ConsumerTopic,
			GroupID:       Config.GroupID,
			MinBytes:      Config.MinBytes,
			MaxBytes:      Config.MaxBytes,
		},
	}

	// creacion del storage, se va a dediccar a enviar los eventos al otro topic de kafka que seran consumidos por HDFS
	storage := &storage.Service{
		StorageConfig: config.StorageConfig{
			Brokers:       Config.Brokers,
			ProducerTopic: Config.ProducerTopic,
			ConsumerTopic: Config.ConsumerTopic,
			RetryDelays:   retryDurations,
		},
	}

	// creacion del transformer, se va a dedicar a procesar los datos recibidos y agregar tags para identificar esa informacion y darle valor
	transformer := &transformer.Service{
		TransformerConfig: config.TransformerConfig{
			Keywords: Datasets.Keywords,
			Patterns: Datasets.Patterns,
		},
	}

	// creacion del indexer, se va a dedicar a orquestar el proceso de transformacion, almacenamiento y recepcion de los datos
	indexer := &indexer.Handler{
		IndexerConfig: config.IndexerConfig{
			Workers: Config.Workers,
		},
		ConsumerService:    consumer,
		StorageService:     storage,
		TransformerService: transformer,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	indexer.InitIndexer(ctx)
}
