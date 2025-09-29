package transformer

import (
	models "minimal-indexer/Models"
	"minimal-indexer/modules/config"
)

type Service struct {
	TransformerConfig config.TransformerConfig
}

func (s *Service) Transform(payload models.KafkaCrawlerPayload) ([]byte, error) {
	return []byte{}, nil
}
