package main

import (
	"errors"
	"fmt"
	"os"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "wherry:", err)
		os.Exit(exitCode(err))
	}
}

// exitCode is the code wherry exits with for err: an *exitError's, else 1.
func exitCode(err error) int {
	if exit := (*exitError)(nil); errors.As(err, &exit) {
		return exit.code
	}
	return 1
}
