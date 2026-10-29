// Command protocol10 is built against the plugin SDK as it was before VersionedPlugins, when Serve and Launch named
// protocol version 10 through HandshakeConfig.ProtocolVersion. Without arguments it serves a plugin whose profiles
// report version v0.0.0-protocol10; given a plugin binary it launches it, configures profile a and prints the plugin's
// version and the negotiated protocol version.
package main

import (
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
		pluginsdk.Version = "v0.0.0-protocol10"
		pluginsdk.Serve(func(pluginsdk.Settings, hclog.Logger) (pluginsdk.Plugin, error) { return profile{}, nil })
		return
	}
	p, err := pluginsdk.Launch(func() (string, error) { return os.Args[1], nil }, os.Stderr)
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
