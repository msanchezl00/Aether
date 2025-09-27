package storage

type StorageInterface interface {
	Storage(data []byte) (bool, error)
}
