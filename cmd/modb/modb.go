package main

import (
	"flag"
	"log"
	"os"
)

type Opts struct {
	Command   string
	Pathname  string
	Datastore string
	Help      bool
}

func main() {
	var err error

	if len(os.Args) == 1 {
		_ = CmdHelp("", Opts{})
		return
	}

	// process the command, incoming args, and any path provided
	opts := Opts{
		Command: os.Args[1],
	}

	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	flagSet.StringVar(&opts.Datastore, "datastore", "bbolt", "the type of store to use; valid: bbolt, badger, level (default: bbolt)")
	flagSet.BoolVar(&opts.Help, "help", false, "help for MoDB")
	flagSet.Parse(os.Args[2:])

	// get any remaining args
	args := flagSet.Args()
	if len(args) > 0 {
		opts.Pathname = args[0]
	}

	// call the correct command
	switch opts.Command {
	case "--help":
		opts.Command = opts.Pathname
		opts.Pathname = ""
		err = CmdHelp("", opts)
	case "help":
		opts.Command = opts.Pathname
		opts.Pathname = ""
		err = CmdHelp("", opts)
	case "server":
		err = CmdServer(opts)
	case "dump":
		err = CmdDump(opts)
	default:
		err = CmdHelp("Unknown command", opts)
	}

	if err != nil {
		log.Fatal("Error running command:", err)
	}
}
