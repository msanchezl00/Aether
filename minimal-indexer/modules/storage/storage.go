package storage

import "minimal-indexer/modules/config"

type Service struct {
	StorageConfig config.StorageConfig
}

func (s *Service) Storage(url string, timeout float32) (bool, error) {
	return true, nil
}
