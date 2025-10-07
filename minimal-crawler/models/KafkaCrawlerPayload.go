package models

import "time"

type KafkaCrawlerPayload struct {
	URL       string         `json:"url"`
	Timestamp time.Time      `json:"timestamp"`
	Content   ContentPayload `json:"content"`
}
