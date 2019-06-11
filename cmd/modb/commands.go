package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/chilts/sid"
	"github.com/gomodule/redigo/redis"
	"github.com/modb-dev/modb/store"
	badger "github.com/modb-dev/modb/store/badger"
	bbolt "github.com/modb-dev/modb/store/bbolt"
	"github.com/tidwall/redcon"
)

var addr = ":29876"
var logBucketName = []byte("log")
var keyBucketName = []byte("key")

func CmdHelp(args ...string) error {
	if len(args) > 0 {
		command := args[0]

		if command == "server" {
			CmdHelpServer()
		}

		return nil
	}

	fmt.Println("MoDB server, client and utilities.")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("")
	fmt.Println("  modb [command]")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println("")
	fmt.Println("  server      start a server")
	fmt.Println("  client      client cli to the server")
	fmt.Println("  dump        dump a database")
	fmt.Println("  help        Help about any command")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("")
	fmt.Println("  -h, --help")
	fmt.Println("        help for modb")
	fmt.Println("")
	fmt.Println("Use 'modb help [command]' for more information about a command.")
	return nil
}

func CmdHelpServer() error {
	fmt.Println("Start an MoDB server node to join to a cluster.")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("")
	fmt.Println("  modb server [fileOrDirName]")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("")
	fmt.Println("  -h, --help")
	fmt.Println("        help for server")
	fmt.Println("")
	fmt.Println("  -s, --store")
	fmt.Println("        help for server")
	fmt.Println("")
	fmt.Println("Use 'modb help [command]' for more information about a command.")
	return nil
}

func CmdServer(args ...string) error {
	if len(args) < 1 {
		return CmdHelpServer()
	}

	filename := args[0]

	log.Println("MoDB Started")
	defer log.Println("MoDB Finished\n")

	var db store.Storage
	var err error

	// open the MoDB database
	isBolt := true
	if isBolt {
		db, err = bbolt.Open(filename)
	} else {
		db, err = badger.Open(filename)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// the main client
	log.Println("Creating Client Server")
	server := redcon.NewServer(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "time":
				t := time.Now().UTC().Format("2006-01-02T15:04:05.999")
				if err != nil {
					conn.WriteError("ERR returning time")
					return
				}
				for len(t) < 23 {
					t = t + "0"
				}
				conn.WriteString(t + "Z\n")

			case "id":
				// id
				conn.WriteString(sid.IdBase64())

			case "set":
				// set <key> <json>
				Set(db, conn, cmd.Args[1:]...)

			case "inc":
				// inc <key> <field>
				// inc chilts logins
				Inc(db, conn, cmd.Args[1:]...)

			case "add":
				// add <key> <field> <count> [<field> <count>...]
				Add(db, conn, cmd.Args[1:]...)

			case "dump":
				Dump(db, conn, cmd.Args[1:]...)

			case "quit":
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
	return server.ListenAndServe()
}

func CmdClient(args ...string) error {
	// connect to the default local address for now
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// print prompt
	fmt.Print("modb> ")

	// scan Stdin
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "quit" {
			break
		}

		// Split string
		r := csv.NewReader(strings.NewReader(text))
		r.Comma = ' ' // space
		fields, err := r.Read()
		if err != nil {
			fmt.Println("(error) ERR syntax error")
			// fmt.Println(err)
			break
		}

		fmt.Printf("\nFields:\n")
		for _, field := range fields {
			fmt.Printf("%q\n", field)
		}

		// split := strings.SplitN(text, " ", 2)
		cmd := fields[0]
		rest := fields[1:]

		// fmt.Printf("text=%s\n", cmd)
		// fmt.Printf("rest=%s\n", rest)

		// ToDo: perform this command
		val, err := redis.String(conn.Do(cmd, rest[0], rest[1]))
		if err != nil {
			fmt.Printf("(error) %s\n", err)
		} else {
			fmt.Printf("-> %s\n", val)
		}

		fmt.Print("modb> ")
	}

	return scanner.Err()
}
