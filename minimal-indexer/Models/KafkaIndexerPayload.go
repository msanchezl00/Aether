package models

type KafkaIndexerPayload struct {
	Domain         string         `json:"domain"`
	Path           string         `json:"path"`
	Date           string         `json:"date"`
	Tags           []string       `json:"tags"`
	ContentPayload ContentPayload `json:"content"`
}
