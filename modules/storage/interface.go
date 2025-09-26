package storage

type StorageInterface interface {
	Storage(payload []map[string]map[string][]string) error
}
