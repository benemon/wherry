// Command protocol11 is built against the plugin SDK release that served protocol version 11, the one before
// ReadWorking. Without arguments it serves a plugin whose profiles report version v0.0.0-protocol11; given a plugin
// binary it launches it, configures profile a and prints the plugin's version and the negotiated protocol version.
package main

import (
	"crypto/sha256"
	"fmt"
	"os"

	"github.com/benemon/wherry/pluginsdk"
	"github.com/hashicorp/go-hclog"
)

// profile implements only what Configure calls.
type profile struct{ pluginsdk.Plugin }

func (profile) Directories() pluginsdk.Directories { return pluginsdk.Directories{} }
func (profile) InlineLimit() int64                 { return 0 }

func main() {
	if len(os.Args) == 1 {
		pluginsdk.Version = "v0.0.0-protocol11"
		pluginsdk.Serve(func(pluginsdk.Settings, hclog.Logger) (pluginsdk.Plugin, error) { return profile{}, nil })
		return
	}
	binary, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pin := sha256.Sum256(binary)
	p, err := pluginsdk.Launch("protocol11", func() (string, error) { return os.Args[1], nil }, pin[:], os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer p.Close()
	if _, err := p.Configure(pluginsdk.Settings{Name: "a", Config: []byte("{}")}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	version, protocol := p.Version()
	fmt.Println(version, protocol)
}
