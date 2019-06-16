package level

import (
	"github.com/chilts/sid"
	"github.com/modb-dev/modb/store"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var separator = ":"
var logPrefix = "log" + separator
var keyPrefix = "key" + separator

type levelStore struct{ db *leveldb.DB }

func Open(filename string) (store.Storage, error) {
	var err error

	db, err := leveldb.OpenFile(filename, nil)
	if err != nil {
		return nil, err
	}

	return &levelStore{db}, nil
}

// Generic 'op'.
func (s *levelStore) op(key, op, json string) error {
	id := sid.IdBase64()
	val := key + ":" + op + ":" + json

	return s.db.Put([]byte(logPrefix+id), []byte(val), nil)
}

// Puts the JSON to the key provided (an overwrite).
func (s *levelStore) Put(key, json string) error {
	return s.op(key, "put", json)
}

// Incs a field inside the object.
func (s *levelStore) Inc(key, json string) error {
	return s.op(key, "inc", json)
}

// Adds to various fields.
func (s *levelStore) Add(key, json string) error {
	return s.op(key, "inc", json)
}

// Deletes the key, which is essentially the same as `put key {}`. The JSON can
// contain anything (such as a reason or message), but it will be ignored when
// reconciling the object's ledger.
func (s *levelStore) Del(key, json string) error {
	return s.op(key, "del", json)
}

func (s *levelStore) IterateLogs(fn func(key, val string)) error {
	r := util.Range{
		Start: []byte(logPrefix),
		// two separators "log::" to signify the end of the range
		Limit: []byte(logPrefix + separator),
	}

	iter := s.db.NewIterator(&r, nil)
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()
		fn(string(key), string(val))
	}

	return nil
}

func (s *levelStore) IterateKeys(fn func(key, val string)) error {
	r := util.Range{
		Start: []byte(keyPrefix),
		// two separators "key::" to signify the end of the range
		Limit: []byte(keyPrefix + separator),
	}

	iter := s.db.NewIterator(&r, nil)
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()
		fn(string(key), string(val))
	}

	return nil
}

// Closes the datastore.
func (s *levelStore) Close() error {
	return s.db.Close()
}
