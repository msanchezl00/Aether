package storage

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	config "minimal-crawler/modules/config"

	"github.com/segmentio/kafka-go"
)

type Service struct {
	writer        *kafka.Writer
	once          sync.Once
	StorageConfig config.StorageConfig
}

func (s *Service) initWriter() {
	s.once.Do(func() {
		s.writer = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  s.StorageConfig.Brokers,
			Balancer: &kafka.LeastBytes{},
		})
	})
}

func (s *Service) Storage(payload []map[string]map[string][]string) error {
	s.initWriter()

	// serializar a JSON
	value, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Topic: s.StorageConfig.ProducerTopic,
		Key:   []byte("key-" + s.StorageConfig.ProducerTopic),
		Value: value,
		Time:  time.Now().UTC(),
	}

	return s.writer.WriteMessages(context.Background(), msg)
}

func (s *Service) Close() {
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			log.Printf("error cerrando writer: %v", err)
		}
	}
}
