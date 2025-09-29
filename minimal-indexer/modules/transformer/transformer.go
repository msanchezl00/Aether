package transformer

import (
	models "minimal-indexer/Models"
	"minimal-indexer/modules/config"
)

type Service struct {
	TransformerConfig config.TransformerConfig
}

func (s *Service) Transform(payload models.KafkaCrawlerPayload) (models.KafkaIndexerPayload, error) {
	return models.KafkaIndexerPayload{}, nil
}
