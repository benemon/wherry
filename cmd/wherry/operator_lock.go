package main

import (
	"context"
	json "encoding/json/v2"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
)

type operatorLockHolder struct {
	Command string    `json:"command"`
	Verb    string    `json:"verb"`
	PID     int       `json:"pid"`
	Host    string    `json:"host"`
	Since   time.Time `json:"since"`
}

// lockOperator takes operator.lock at path for the operator command verb with args, waiting without limit while
// another command holds it, and records the holder for the next one to name.
func lockOperator(path, verb string, args []string, out io.Writer) (func(), error) {
	release, err := busclient.LockFile(context.Background(), path, func(contents []byte) {
		var holder operatorLockHolder
		if json.Unmarshal(contents, &holder) != nil {
			fmt.Fprintln(out, "waiting for another operator command")
			return
		}
		fmt.Fprintf(out, "waiting for %s (pid %d on %s, since %s)", holder.Command, holder.PID, holder.Host, holder.Since.Format(time.RFC3339))
		if holder.Verb == "revoke" || holder.Verb == "remove" {
			fmt.Fprintln(out, "; to go first, interrupt that command and run it again afterwards")
		} else {
			fmt.Fprintln(out, "; do not interrupt it: an approve that is interrupted, or that ends on a timeout, can leave an upsert the broker makes later")
		}
	})
	if err != nil {
		return nil, err
	}
	host, err := os.Hostname()
	if err == nil {
		var encoded []byte
		encoded, err = json.Marshal(operatorLockHolder{Command: "wherry operator " + strings.Join(args, " "), Verb: verb, PID: os.Getpid(), Host: host, Since: time.Now().UTC()})
		if err == nil {
			err = os.WriteFile(path, encoded, 0o600)
		}
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
}
