package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/oklog/run"
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

	// create a context that can be cancelled
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a run.Group to run all actors in order
	var group run.Group

	// Ctrl-C
	{
		group.Add(func() error {
			log.Println("Listening for C-c")
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt)
			<-c
			log.Println("Ctrl-c - Shutting down")
			return nil
		}, func(error) {
			log.Println("Cancelling context")
			cancel()
		})
	}

	// Datastore
	db, err := NewStore(datastore, pathname)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("Closing Datastore")
		db.Close()
	}()

	// Client Server
	var server *redcon.Server
	{
		addr := ":29876"

		group.Add(func() error {
			log.Println("Creating Client Server")
			server = NewClientServer(addr, db)
			log.Printf("Client Server about to listen on %s\n", addr)
			return server.ListenAndServe()
		}, func(error) {
			log.Println("Closing Client Server")
			server.Close()
		})
	}

	return group.Run()
}
