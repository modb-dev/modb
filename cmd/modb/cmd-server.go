package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/chilts/sid"
	"github.com/modb-dev/modb/store"
	badger "github.com/modb-dev/modb/store/badger"
	bbolt "github.com/modb-dev/modb/store/bbolt"
	"github.com/tidwall/redcon"
)

func CmdHelpServer(msg string) error {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}
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
	fmt.Println("  -d, --datastore")
	fmt.Println("        help for server")
	fmt.Println("")
	fmt.Println("Use 'modb help [command]' for more information about a command.")
	return nil
}

func CmdServer(arguments ...string) error {
	flagSet := flag.NewFlagSet("", flag.ContinueOnError)

	// store type
	var datastore string
	flagSet.StringVar(&datastore, "datastore", "bbolt", "the type of store to use; valid: bbolt, badger (default: bbolt)")

	// help
	var help bool
	flagSet.BoolVar(&help, "help", false, "help for MoDB")

	flagSet.Parse(arguments)

	if help == true {
		return CmdHelpServer("")
	}

	// get any remaining args
	args := flagSet.Args()
	if len(args) < 1 {
		return CmdHelpServer("Provide a path for your datastore")
	}

	// the database pathname to open (file for bbolt, directory for badger)
	pathname := args[0]

	log.Println("MoDB Started")
	defer log.Println("MoDB Finished\n")

	var db store.Storage
	var err error

	// open the MoDB database
	if datastore == "bbolt" {
		log.Printf("Using datastore bbolt")
		db, err = bbolt.Open(pathname)
	} else if datastore == "badger" {
		log.Printf("Using datastore badger")
		db, err = badger.Open(pathname)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

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

			case "put":
				// put <key> <json>
				Put(db, conn, cmd.Args[1:]...)

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
