package main

import (
	"fmt"
	"log"
	"time"

	bolt "go.etcd.io/bbolt"
)

func main() {
	fmt.Println("MoDB Started")
	defer fmt.Println("MoDB Finished\n")

	// open the MoDB database
	db, err := bolt.Open("modb.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// create the various buckets
	err = db.Update(func(tx *bolt.Tx) error {
		// key
		_, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return fmt.Errorf("create key bucket: %s", err)
		}

		return nil
	})

}
