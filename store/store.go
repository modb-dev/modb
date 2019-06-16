package store

// Change is a tuple of key, id, op, and diff.
type Change struct {
	Key  string
	Id   string
	Op   string
	Diff string
}

type Storage interface {
	Put(key, json string) error
	Inc(key, json string) error
	Add(key, json string) error
	Del(key, json string) error
	// Get(key string) error
	IterateChanges(key string, fn func(change Change)) error
	IterateLog(fn func(key, val string)) error
	IterateData(fn func(key, val string)) error
	Close() error
}
