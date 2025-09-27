package storage

import (
	"context"
	"fmt"
	"log"
	"sync"

	config "minimal-indexer/modules/config"

	"github.com/segmentio/kafka-go"
)

type Service struct {
	reader        *kafka.Reader
	once          sync.Once
	StorageConfig config.StorageConfig
}

func (s *Service) initReader() {
	s.once.Do(func() {
		s.reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers: s.StorageConfig.Brokers,       // Dirección de Kafka
			Topic:   s.StorageConfig.ConsumerTopic, // Nombre del tópico
			GroupID: "consumer-group",              // ID de grupo de consumidores
		})
	})
}

// recursivo con índice de intento
func (s *Service) KafkaConsumer() error {
	// Leer mensajes del tópico
	for {
		m, err := s.reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("Error al leer mensaje: %v\n", err)
		}
		// Mostrar el mensaje recibido
		fmt.Printf("Mensaje recibido: Key: %s, Value: %s\n", string(m.Key), string(m.Value))
	}
}
