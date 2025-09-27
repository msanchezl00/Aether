package storage

import (
	"context"
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

// recursivo con índice de intento
func (s *Service) KafkaStorage(payload []byte, attempt int) error {
	s.initWriter()

	msg := kafka.Message{
		Topic: s.StorageConfig.ProducerTopic,
		Key:   nil,
		Value: payload,
		Time:  time.Now().UTC(),
	}

	err := s.writer.WriteMessages(context.Background(), msg)
	if err == nil {
		return nil
	}

	if attempt >= len(s.StorageConfig.RetryDelays) {
		log.Printf("error final enviando a Kafka: %v", err)
		return err
	}

	delay := s.StorageConfig.RetryDelays[attempt]
	log.Printf("error enviando a Kafka, reintentando en %s: %v", delay, err)
	time.Sleep(delay)

	return s.KafkaStorage(payload, attempt+1)
}

func (s *Service) Close() {
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			log.Printf("error cerrando writer: %v", err)
		}
	}
}
