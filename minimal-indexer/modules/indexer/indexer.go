package indexer

import (
	config "minimal-indexer/modules/config"
)

type Service struct {
	IndexerConfig config.IndexerConfig
}

func (s *Service) InitIndexer(url string, timeout float32) (bool, error) {
	return true, nil
}
