package main

import (
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/pluginsdk"
)

// The receiver process registers these wherry_ series beside the receiver's families; hostSeries in
// internal/receiver's observability test lists the same names.
func TestHostSeries(t *testing.T) {
	registry := newRegistry()
	plugins := &plugins{dir: installPlugin(t), logs: io.Discard, logger: discard, registry: registry, running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	if _, err := plugins.profile("build", admin.ProfileSettings{Name: "exec", Plugin: "fake", Agent: "claude"}); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "wherry_") {
			names = append(names, family.GetName())
		}
	}
	if want := []string{"wherry_build_info", "wherry_plugin_restarts_total"}; !slices.Equal(names, want) {
		t.Fatalf("host series %v, want %v", names, want)
	}
}
