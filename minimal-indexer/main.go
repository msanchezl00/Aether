package main

import (
	"encoding/json"
	"io"
	config "minimal-indexer/modules/config"
	"os"
)

// estructura de la configuracion del json
var Config struct {
	Brokers       []string `json:"brokers"`
	ProducerTopic string   `json:"producer-topic"`
	ConsumerTopic string   `json:"consumer-topic"`
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

}
