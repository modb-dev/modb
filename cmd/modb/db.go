package main

import (
	"errors"
	"log"

	"github.com/modb-dev/modb/store"
	"github.com/modb-dev/modb/store/badger"
	"github.com/modb-dev/modb/store/bbolt"
	"github.com/modb-dev/modb/store/level"
)

func NewStore(datastore, pathname string) (store.Storage, error) {
	// open the MoDB database
	if datastore == "bbolt" {
		log.Printf("Using datastore bbolt")
		return bbolt.Open(pathname)
	}

	if datastore == "badger" {
		log.Printf("Using datastore badger")
		return badger.Open(pathname)
	}

	if datastore == "level" {
		log.Printf("Using datastore level")
		return level.Open(pathname)
	}

	return nil, errors.New("Unknown datastore")
}
