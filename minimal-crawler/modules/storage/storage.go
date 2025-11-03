package storage

import (
	"context"
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
		config.Logger.Infof("Conectando a brokers: %v\n", s.StorageConfig.Brokers)
		s.writer = kafka.NewWriter(kafka.WriterConfig{
			Brokers:    s.StorageConfig.Brokers,
			BatchBytes: 50 * 1024 * 1024, // 50 MB máximo por batch
			Balancer:   &kafka.LeastBytes{},
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
		config.Logger.Errorf("error final enviando a Kafka: %v", err)
		return err
	}

	delay := s.StorageConfig.RetryDelays[attempt]
	config.Logger.Errorf("error enviando a Kafka, reintentando en %s: %v", delay, err)
	time.Sleep(delay)

	return s.KafkaStorage(payload, attempt+1)
}

func (s *Service) Close() {
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			config.Logger.Errorf("error cerrando writer: %v", err)
		}
	}
}
