package bbolt

import (
	"fmt"
	"time"

	"github.com/chilts/sid"
	"github.com/modb-dev/modb/store"
	bbolt "go.etcd.io/bbolt"
)

var logBucketName = []byte("log")
var keyBucketName = []byte("key")

type bboltStore struct{ db *bbolt.DB }

func Open(filename string) (store.Storage, error) {
	var err error

	db, err := bbolt.Open(filename, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	// create the various buckets
	err = db.Update(func(tx *bbolt.Tx) error {
		// log
		_, err := tx.CreateBucketIfNotExists(logBucketName)
		if err != nil {
			return fmt.Errorf("create log bucket: %s", err)
		}

		// key
		_, err = tx.CreateBucketIfNotExists(keyBucketName)
		if err != nil {
			return fmt.Errorf("create key bucket: %s", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &bboltStore{db}, nil
}

// Generic 'op'.
func (s *bboltStore) op(key, op, json string) error {
	id := sid.IdBase64()
	keyKey := key + ":" + id
	logKey := id + ":" + key
	val := op + ":" + json

	return s.db.Update(func(tx *bbolt.Tx) error {
		lb := tx.Bucket(logBucketName)
		err := lb.Put([]byte(logKey), []byte(val))
		if err != nil {
			return fmt.Errorf("put log bucket: %s", err)
		}

		kb := tx.Bucket(keyBucketName)
		err = kb.Put([]byte(keyKey), []byte(val))
		if err != nil {
			return fmt.Errorf("put key bucket: %s", err)
		}

		return nil
	})
}

// Sets the item to the json data provided.
func (s *bboltStore) Set(key, json string) error {
	return s.op(key, "set", json)
}

// Incs a field inside the object.
func (s *bboltStore) Inc(key, json string) error {
	return s.op(key, "inc", json)
}

// Adds to various fields.
func (s *bboltStore) Add(key, json string) error {
	return s.op(key, "inc", json)
}

func (s *bboltStore) Iterate(fn func(key, val string)) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		kb := tx.Bucket(keyBucketName)
		c := kb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fn(string(k), string(v))
		}
		return nil
	})
}

// Closes the datastore.
func (s *bboltStore) Close() error {
	return s.db.Close()
}
