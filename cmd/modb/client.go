package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/modb-dev/modb/store"
	"github.com/tidwall/redcon"
	"github.com/tidwall/sjson"
)

func Set(db store.Storage, conn redcon.Conn, args ...[]byte) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments: set <key> <json>")
		return
	}

	// key is any string, val should be a valid JSON object
	key := string(args[0])
	val := string(args[1])

	// ToDo: validate both name and json.
	err := db.Set(key, val)
	if err != nil {
		log.Printf("set: db.Set(): err: ", err)
		conn.WriteError("ERR writing to datastore")
		return
	}

	conn.WriteString("OK")
}

func Inc(db store.Storage, conn redcon.Conn, args ...[]byte) {
	// Usage:
	// > inc chilts logins

	// Sugar for:
	// > add chilts logins 1

	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments: inc <key> <field>")
		return
	}

	newArgs := [][]byte{args[0], args[1], []byte("1")}
	Add(db, conn, newArgs...)
}

func Add(db store.Storage, conn redcon.Conn, args ...[]byte) {
	// Usage:
	// > add chilts logins 1 [field count...]

	if len(args) < 3 {
		conn.WriteError("ERR wrong number of arguments: add <key> <field> <count> [<field> <count>...]")
		return
	}

	if (len(args) % 2) == 0 {
		conn.WriteError("ERR wrong number of arguments: add <key> <field> <count> [<field> <count>...]")
		return
	}

	// ToDo: validate incoming `args`
	key := string(args[0])
	json := "{}"
	for i := 1; i < len(args); i += 2 {
		field := string(args[i])
		count, err := strconv.Atoi(string(args[i+1]))
		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR invalid count '%s' at argument %d", string(args[i+1]), 2+i+1))
			return
		}
		json, err = sjson.Set(json, field, count)
		if err != nil {
			log.Printf("error creating json, err:", err)
			conn.WriteError("ERR error creating JSON")
			return
		}
	}

	err := db.Add(key, json)
	if err != nil {
		log.Printf("set: db.Add(): err: ", err)
		conn.WriteError("ERR writing to datastore")
		return
	}

	conn.WriteString("OK")
}

func Dump(db store.Storage, conn redcon.Conn, args ...[]byte) {
	fmt.Println("+++ Dump +++")
	db.Iterate(func(key, val string) {
		fmt.Printf("* %s=%s\n", key, val)
	})
	fmt.Println("--- Dump ---")

	conn.WriteString("DONE")
}