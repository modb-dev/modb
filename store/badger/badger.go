package badger

import (
	"fmt"
	"log"
	"strings"

	"github.com/chilts/sid"
	"github.com/dgraph-io/badger"
	"github.com/modb-dev/modb/store"
)

var logPrefix = "log:"
var keyPrefix = "key:"

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

// op
func (s *badgerStore) op(key, op, json string) error {
	id := sid.IdBase64()
	val := key + ":" + op + ":" + json

	return s.db.Update(func(txn *badger.Txn) error {
		// log
		err := txn.Set([]byte(logPrefix+id), []byte(val))
		if err != nil {
			return fmt.Errorf("put log bucket: %s", err)
		}

		return nil
	})
}

// Puts the JSON to the key provided (an overwrite).
func (s *badgerStore) Put(key, json string) error {
	return s.op(key, "put", json)
}

// Incs a field in the object.
func (s *badgerStore) Inc(key, json string) error {
	return s.op(key, "inc", json)
}

// Adds to various fields.
func (s *badgerStore) Add(key, json string) error {
	return s.op(key, "add", json)
}

// Deletes the key, which is essentially the same as `put key {}`. The JSON can
// contain anything (such as a reason or message), but it will be ignored when
// reconciling the object's ledger.
func (s *badgerStore) Del(key, json string) error {
	return s.op(key, "del", json)
}

func (s *badgerStore) IterateLogs(fn func(key, val string)) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		prefix := []byte(logPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			val, err := item.Value()
			if err != nil {
				return err
			}
			fn(strings.TrimPrefix(string(key), keyPrefix), string(val))
		}
		return nil
	})
}

func (s *badgerStore) IterateKeys(fn func(key, val string)) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		prefix := []byte(keyPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			val, err := item.Value()
			if err != nil {
				return err
			}
			fn(strings.TrimPrefix(string(key), keyPrefix), string(val))
		}
		return nil
	})
}

// Closes the datastore.
func (s *badgerStore) Close() error {
	return s.db.Close()
}
