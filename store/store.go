package store

type Storage interface {
	Set(id, key, val string) error
	// Get(key string) error
	Iterate(fn func(key, val string)) error
	Close() error
}
