package main

import (
	"log"
	"os"
)

func main() {
	var err error

	if len(os.Args) == 1 {
		_ = CmdHelp("")
		return
	}

	command := os.Args[1]

	switch command {
	case "--help":
		err = CmdHelp("", os.Args[2:]...)
	case "help":
		err = CmdHelp("", os.Args[2:]...)
	case "server":
		err = CmdServer(os.Args[2:]...)
	default:
		err = CmdHelp("Unknown command", os.Args[2:]...)
	}

	if err != nil {
		log.Fatal("Error running command:", err)
	}
}
