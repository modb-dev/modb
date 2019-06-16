package bbolt

import (
	"fmt"
	"strings"
	"time"

	"github.com/chilts/sid"
	"github.com/modb-dev/modb/store"
	bbolt "go.etcd.io/bbolt"
)

var separator = ":"
var logBucketName = []byte("log")
var dataBucketName = []byte("data")

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

		// data
		_, err = tx.CreateBucketIfNotExists(dataBucketName)
		if err != nil {
			return fmt.Errorf("create data bucket: %s", err)
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
	id := key + separator + sid.IdBase64()
	val := op + separator + json

	return s.db.Update(func(tx *bbolt.Tx) error {
		kb := tx.Bucket(logBucketName)
		err := kb.Put([]byte(id), []byte(val))
		if err != nil {
			return fmt.Errorf("put key bucket: %s", err)
		}

		return nil
	})
}

// Puts the JSON to the key provided (an overwrite).
func (s *bboltStore) Put(key, json string) error {
	return s.op(key, "put", json)
}

// Incs a field inside the object.
func (s *bboltStore) Inc(key, json string) error {
	return s.op(key, "inc", json)
}

// Adds to various fields.
func (s *bboltStore) Add(key, json string) error {
	return s.op(key, "inc", json)
}

// Deletes the key, which is essentially the same as `put key {}`. The JSON can
// contain anything (such as a reason or message), but it will be ignored when
// reconciling the object's ledger.
func (s *bboltStore) Del(key, json string) error {
	return s.op(key, "del", json)
}

func (s *bboltStore) IterateChanges(key string, fn func(change store.Change)) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		kb := tx.Bucket(logBucketName)
		c := kb.Cursor()
		for k, v := c.Seek([]byte(key)); k != nil; k, v = c.Next() {
			if strings.HasPrefix(string(k), key+separator) {
				id := strings.SplitN(string(k), separator, 2)[1]
				opDiff := strings.SplitN(string(v), separator, 2)
				change := store.Change{
					Key:  key,
					Id:   id,
					Op:   opDiff[0],
					Diff: opDiff[1],
				}
				fn(change)
			}
		}
		return nil
	})
}

func (s *bboltStore) IterateLog(fn func(key, val string)) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		kb := tx.Bucket(logBucketName)
		c := kb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fn(string(k), string(v))
		}
		return nil
	})
}

func (s *bboltStore) IterateData(fn func(key, val string)) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		kb := tx.Bucket(dataBucketName)
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
