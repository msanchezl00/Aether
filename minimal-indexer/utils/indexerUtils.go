package utils

import (
	"encoding/json"
	models "minimal-indexer/Models"
)

func BuildPayload(payload models.KafkaIndexerPayload) []byte {
	value, _ := json.Marshal(payload)
	return value
}
