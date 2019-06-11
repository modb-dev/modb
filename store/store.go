package store

type Storage interface {
	Put(key, json string) error
	Inc(key, json string) error
	Add(key, json string) error
	// Get(key string) error
	Iterate(fn func(key, val string)) error
	Close() error
}
