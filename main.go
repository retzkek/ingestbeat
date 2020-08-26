package main

import (
	"os"

	"github.com/retzkek/ingestbeat/cmd"

	_ "github.com/retzkek/ingestbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
