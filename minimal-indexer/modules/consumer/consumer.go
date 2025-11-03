package consumer

import (
	"context"
	"log"
	"minimal-indexer/modules/config"

	"github.com/segmentio/kafka-go"
)

type Service struct {
	ConsumerConfig config.ConsumerConfig
}

func (s *Service) Consumer(ctx context.Context, handler func(kafka.Message)) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  s.ConsumerConfig.Brokers,
		Topic:    s.ConsumerConfig.ConsumerTopic,
		MinBytes: 10e3,             // 10KB
		MaxBytes: 50 * 1024 * 1024, // 50 MB máximo por fetch
	})

	defer r.Close()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			// si el contexto fue cancelado, se cierra limpio
			if ctx.Err() != nil {
				log.Printf("consumer stopped: %v", ctx.Err())
				return nil
			}
			log.Printf("error leyendo mensaje: %v", err)
			continue
		}
		handler(m)
	}
}
