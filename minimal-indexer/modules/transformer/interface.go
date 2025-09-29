package transformer

import models "minimal-indexer/Models"

type TransformerInterface interface {
	Transform(payload models.KafkaCrawlerPayload) ([]byte, error)
}
