package models

import "time"

type KafkaPayload struct {
	URL       string                           `json:"url"`
	Timestamp time.Time                        `json:"timestamp"`
	Payload   []map[string]map[string][]string `json:"payload"`
}
