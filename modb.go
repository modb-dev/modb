package main

import (
	"fmt"
	"log"
	"time"

	"github.com/tidwall/redcon"
	bolt "go.etcd.io/bbolt"
)

func main() {
	log.Println("MoDB Started")
	defer log.Println("MoDB Finished\n")

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

	// the main client
	addr := ":29876"

	log.Printf("Opening %s", addr)
	err = redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch string(cmd.Args[0]) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			log.Printf("Accept %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			if err != nil {
				log.Printf("Closed %s (err: %v)", conn.RemoteAddr(), err)
				return
			}
			log.Printf("Closed %s", conn.RemoteAddr())
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
