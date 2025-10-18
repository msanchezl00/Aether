package models

type KafkaIndexerPayload struct {
	Domain   string         `avro:"domain"`
	Path     string         `avro:"path"`
	Date     string         `avro:"date"`
	RealPath string         `avro:"real_path"`
	Tags     []string       `avro:"tags"`
	Content  ContentPayload `avro:"content"`
}
