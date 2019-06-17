package main

import (
	"fmt"
	"log"
)

func CmdHelpDump(msg string) error {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}
	fmt.Println("Dump a local datastore to stdout.")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("")
	fmt.Println("  modb dump [fileOrDirName]")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("")
	fmt.Println("  -h, --help")
	fmt.Println("        help for dump")
	fmt.Println("")
	fmt.Println("  -d, --datastore")
	fmt.Println("        path to datastore")
	fmt.Println("")
	fmt.Println("Use 'modb help [command]' for more information about a command.")
	return nil
}

func CmdDump(opts Opts) error {
	if opts.Help == true {
		return CmdHelpServer("")
	}

	if opts.Pathname == "" {
		return CmdHelpDump("Provide a path for your datastore")
	}

	log.Println("MoDB Started")
	defer log.Println("MoDB Finished\n")

	// Datastore
	db, err := NewStore(opts.Datastore, opts.Pathname)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("Closing Datastore")
		db.Close()
	}()

	// ToDo
	log.Println("Dumping datastore ...")

	return nil
}
