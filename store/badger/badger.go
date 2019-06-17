package badger

import (
	"fmt"
	"log"
	"strings"

	"github.com/chilts/sid"
	"github.com/dgraph-io/badger"
	"github.com/modb-dev/modb/store"
)

var separator = ":"
var logPrefix = "log" + separator
var dataPrefix = "data" + separator

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
	id := key + ":" + sid.IdBase64()
	val := op + ":" + json

	return s.db.Update(func(txn *badger.Txn) error {
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

// Inc increments a field in the object.
func (s *badgerStore) Inc(key, json string) error {
	return s.op(key, "inc", json)
}

// IncBy increments various fields with values.
func (s *badgerStore) IncBy(key, json string) error {
	return s.op(key, "incby", json)
}

// Deletes the key, which is essentially the same as `put key {}`. The JSON can
// contain anything (such as a reason or message), but it will be ignored when
// reconciling the object's ledger.
func (s *badgerStore) Del(key, json string) error {
	return s.op(key, "del", json)
}

func (s *badgerStore) IterateChanges(key string, fn func(change store.Change)) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		prefix := []byte(logPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := strings.TrimPrefix(string(item.Key()), logPrefix)
			v, err := item.Value()
			if err != nil {
				return err
			}

			id := strings.SplitN(k, separator, 2)[1]
			opDiff := strings.SplitN(string(v), separator, 2)
			change := store.Change{
				Key:  key,
				Id:   id,
				Op:   opDiff[0],
				Diff: opDiff[1],
			}
			fn(change)
		}
		return nil
	})
}

func (s *badgerStore) IterateLog(fn func(key, val string)) error {
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
			fn(strings.TrimPrefix(string(key), logPrefix), string(val))
		}
		return nil
	})
}

func (s *badgerStore) IterateData(fn func(key, val string)) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		prefix := []byte(dataPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			val, err := item.Value()
			if err != nil {
				return err
			}
			fn(strings.TrimPrefix(string(key), dataPrefix), string(val))
		}
		return nil
	})
}

// Closes the datastore.
func (s *badgerStore) Close() error {
	return s.db.Close()
}
