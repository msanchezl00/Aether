package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func consumer() {
	// Configuración del lector Kafka (Consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"}, // Dirección de Kafka
		Topic:   "crawled_data",             // Nombre del tópico
		GroupID: "consumer-group",           // ID de grupo de consumidores
	})

	// Leer mensajes del tópico
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("Error al leer mensaje: %v\n", err)
		}
		// Mostrar el mensaje recibido
		fmt.Printf("Mensaje recibido: Key: %s, Value: %s\n", string(m.Key), string(m.Value))
	}

	// Cerrar el lector (no se llega aquí debido al bucle infinito)
	defer reader.Close()
}
