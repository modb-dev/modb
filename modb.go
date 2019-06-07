package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/chilts/sid"
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
		_, err := tx.CreateBucketIfNotExists([]byte("key"))
		if err != nil {
			return fmt.Errorf("create key bucket: %s", err)
		}

		return nil
	})

	// the main client
	addr := ":29876"
	log.Println("Creating Client Server")
	server := redcon.NewServer(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "time":
				t := time.Now().Format(time.RFC3339)
				if err != nil {
					conn.WriteError("ERR returning time")
					return
				}
				conn.WriteString(t + "\n")
			case "id":
				// conn.WriteString(sid.IdHex())
				conn.WriteString(sid.IdBase64())
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments: set key val")
					return
				}

				// key is any string, val should be a valid JSON object
				key := string(cmd.Args[1])
				val := string(cmd.Args[2])

				// ToDo: validate both name and json.

				err = db.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("key"))

					id := sid.IdBase64()

					fmt.Printf("id=%s\n", id)

					err := b.Put([]byte(key), []byte(val))
					if err != nil {
						return fmt.Errorf("create key bucket: %s", err)
					}
					return nil
				})
				if err != nil {
					log.Printf("set: db.Update(): err: ", err)
					conn.WriteError("ERR writing to datastore")
					return
				}

				conn.WriteString("OK")

			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments: get key")
					return
				}

				// key is any string, val should be a valid JSON object
				key := string(cmd.Args[1])

				// ToDo: validate both name and json.

				// ToDo: get isn't this simple, we need to iterate over all values for this key!!
				// And of course, talk to the other nodes if requested.

				var val []byte
				err = db.View(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("key"))

					val = b.Get([]byte(key))
					return nil
				})
				if err != nil {
					log.Printf("get: db.View(): err: ", err)
					conn.WriteError("ERR reading from datastore")
					return
				}

				if val == nil {
					conn.WriteString("nil")
					return
				}
				conn.WriteString(string(val))

			case "dump":
				// ToDo: ... ???
				conn.WriteString("ToDo!")
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

	log.Printf("Client Server listening on %s\n", addr)
	err = server.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}
