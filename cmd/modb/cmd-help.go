package main

import (
	"fmt"
)

func CmdHelp(msg string, opts Opts) error {
	// get any remaining args
	if opts.Command != "" {
		if opts.Command == "server" {
			CmdHelpServer(msg)
		}

		return nil
	}

	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
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
