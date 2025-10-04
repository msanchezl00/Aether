package models

type KafkaIndexerPayload struct {
	Domain  string                           `json:"domain"`
	Path    string                           `json:"path"`
	Date    string                           `json:"date"`
	Tags    []string                         `json:"tags"`
	Content []map[string]map[string][]string `json:"content"`
}
