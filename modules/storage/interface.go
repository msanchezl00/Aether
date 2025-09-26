package storage

type StorageInterface interface {
	KafkaStorage(payload []byte, attempt int) error
}
