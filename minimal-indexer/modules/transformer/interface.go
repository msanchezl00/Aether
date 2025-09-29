package transformer

import models "minimal-indexer/Models"

type TransformerInterface interface {
	Transform(payload models.KafkaCrawlerPayload) (models.KafkaIndexerPayload, error)
}
