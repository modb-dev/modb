package level

import (
	"strings"

	"github.com/chilts/sid"
	"github.com/modb-dev/modb/store"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var separator = ":"
var endSeparator = "\xff"
var logPrefix = "log" + separator
var dataPrefix = "data" + separator

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
	id := key + ":" + sid.IdBase64()
	val := op + ":" + json

	return s.db.Put([]byte(logPrefix+id), []byte(val), nil)
}

// Puts the JSON to the key provided (an overwrite).
func (s *levelStore) Put(key, json string) error {
	return s.op(key, "put", json)
}

// Increments a field inside the object.
func (s *levelStore) Inc(key, json string) error {
	return s.op(key, "inc", json)
}

// Increments a number of fields by respective values.
func (s *levelStore) IncBy(key, json string) error {
	return s.op(key, "incby", json)
}

// Deletes the key, which is essentially the same as `put key {}`. The JSON can
// contain anything (such as a reason or message), but it will be ignored when
// reconciling the object's ledger.
func (s *levelStore) Del(key, json string) error {
	return s.op(key, "del", json)
}

func (s *levelStore) IterateChanges(key string, fn func(change store.Change)) error {
	r := util.Range{
		Start: []byte(logPrefix),
		Limit: []byte(logPrefix + endSeparator),
	}

	iter := s.db.NewIterator(&r, nil)
	for iter.Next() {
		k := strings.TrimPrefix(string(iter.Key()), logPrefix)
		v := string(iter.Value())
		id := strings.SplitN(k, separator, 2)[1]
		opDiff := strings.SplitN(v, separator, 2)
		change := store.Change{
			Key:  key,
			Id:   id,
			Op:   opDiff[0],
			Diff: opDiff[1],
		}
		fn(change)
	}

	return nil
}

func (s *levelStore) IterateLog(fn func(key, val string)) error {
	r := util.Range{
		Start: []byte(logPrefix),
		Limit: []byte(logPrefix + endSeparator),
	}

	iter := s.db.NewIterator(&r, nil)
	for iter.Next() {
		k := strings.TrimPrefix(string(iter.Key()), logPrefix)
		v := string(iter.Value())
		fn(k, v)
	}

	return nil
}

func (s *levelStore) IterateData(fn func(key, val string)) error {
	r := util.Range{
		Start: []byte(dataPrefix),
		Limit: []byte(dataPrefix + endSeparator),
	}

	iter := s.db.NewIterator(&r, nil)
	for iter.Next() {
		k := strings.TrimPrefix(string(iter.Key()), dataPrefix)
		v := string(iter.Value())
		fn(k, v)
	}

	return nil
}

// Closes the datastore.
func (s *levelStore) Close() error {
	return s.db.Close()
}
