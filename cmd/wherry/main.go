package main

import (
	"errors"
	"fmt"
	"os"
)

// quietExit is an error that ends wherry with its value as the exit status and prints nothing.
type quietExit int

func (code quietExit) Error() string { return fmt.Sprintf("exit status %d", int(code)) }

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		var code quietExit
		if errors.As(err, &code) {
			os.Exit(int(code))
		}
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
