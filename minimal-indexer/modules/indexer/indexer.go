package indexer

import (
	config "minimal-indexer/modules/config"
	"minimal-indexer/modules/consumer"
	"minimal-indexer/modules/storage"
	"minimal-indexer/modules/transformer"
)

type Handler struct {
	IndexerConfig      config.IndexerConfig
	TransformerService transformer.TransformerInterface
	StorageService     storage.StorageInterface
	ConsumerService    consumer.ConsumerInterface
}

func (s *Handler) InitIndexer(url string, timeout float32) (bool, error) {
	return true, nil
}

func (h *Handler) Indexer() error {
	return nil
}
