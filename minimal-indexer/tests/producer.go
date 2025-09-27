package tests

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func producer() {
	// Configuración del escritor Kafka (Producer)
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"}, // Dirección de Kafka
		Topic:    "crawled_data",             // Nombre del tópico
		Balancer: &kafka.LeastBytes{},        // Balancer para distribuir los mensajes
	})

	// Enviar un mensaje
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("Mensaje %d", i)
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(message),
			},
		)
		if err != nil {
			log.Fatalf("Error al enviar mensaje: %v\n", err)
		}
		fmt.Printf("Mensaje enviado: %s\n", message)
		time.Sleep(1 * time.Second) // Simular intervalo entre mensajes
	}

	// Cerrar el escritor
	if err := writer.Close(); err != nil {
		log.Fatalf("Error al cerrar el productor: %v\n", err)
	}
}
