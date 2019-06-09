package badger

import (
	"fmt"
	"log"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/modb-dev/modb/store"
)

var logBucketName = []byte("log")
var keyBucketName = []byte("key")

type badgerStore struct{ db *badger.DB }

func Open(dirname string) (store.Storage, error) {
	var err error

	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = dirname
	opts.ValueDir = dirname
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return &badgerStore{db}, nil
}

// Sets the item to the json data provided.
func (s *badgerStore) Set(id, key, json string) error {
	keyKey := key + ":" + id
	logKey := id + ":" + key
	val := "set:" + json

	return s.db.Update(func(txn *badger.Txn) error {
		// log
		err := txn.Set([]byte("log:"+logKey), []byte(val))
		if err != nil {
			return fmt.Errorf("put log bucket: %s", err)
		}

		// key
		err = txn.Set([]byte("key:"+keyKey), []byte(val))
		if err != nil {
			return fmt.Errorf("put key bucket: %s", err)
		}

		return nil
	})
}

func (s *badgerStore) Iterate(fn func(key, val string)) error {
	log.Println("Inside Iterate()")
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		prefix := []byte("key:")
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			val, err := item.Value()
			if err != nil {
				return err
			}
			fn(strings.TrimPrefix(string(key), "key:"), string(val))
		}
		return nil
	})
}

// Closes the datastore.
func (s *badgerStore) Close() error {
	return s.db.Close()
}
