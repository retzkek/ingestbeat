package main

import (
	"os"

	"cdcvs.fnal.gov/landscape/ingestbeat/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
