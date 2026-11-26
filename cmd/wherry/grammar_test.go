package main

import (
	"bytes"
	"slices"
	"strings"
	"testing"
)

// Every verb's forms are walked: the form's resource flags parse; a resource flag beside them fits another form of
// the verb or is the verb's usage error, as is one repeated or empty, --host without --tenant and no resource flag
// where the verb takes one; and an option outside the form is refused by name.
func TestOperatorResourceFlagRules(t *testing.T) {
	options := []string{"file", "dry-run", "force", "timeout", "suspect-key", "broker", "super-user", "super-user-password", "super-user-mechanism", "ca-cert", "window", "port", "external-url"}
	given := func(name string) []string {
		if name == "dry-run" || name == "force" {
			return []string{"--" + name}
		}
		return []string{"--" + name, "x"}
	}
	parse := func(args ...string) string {
		if _, _, err := operatorArgs(args); err != nil {
			return err.Error()
		}
		return ""
	}
	for verb, forms := range operatorForms {
		usage := operatorUsage[verb]
		if usage == "" {
			t.Fatalf("%s has forms and no usage", verb)
		}
		accepts := func(resources []string) bool {
			return slices.ContainsFunc(forms, func(form operatorForm) bool { return slices.Equal(form.resources, resources) })
		}
		if !accepts(nil) && parse(verb) != usage {
			t.Errorf("%s without a resource flag = %q; want its usage", verb, parse(verb))
		}
		for _, form := range forms {
			args := []string{verb}
			if verb == "list" {
				args = append(args, "hosts")
			}
			for _, resource := range form.resources {
				args = append(args, "--"+resource, resource+"-a")
			}
			if got := parse(args...); got != "" {
				t.Errorf("%q = %q; want it accepted", args, got)
			}
			if got := parse(append(slices.Clone(args), "--dir", "d")...); got != "" {
				t.Errorf("%q with --dir = %q; want it accepted", args, got)
			}
			for _, name := range options {
				got, want := parse(append(slices.Clone(args), given(name)...)...), ""
				if !slices.Contains(form.options, name) {
					want = "unknown option --" + name
				}
				if got != want {
					t.Errorf("%q with --%s = %q; want %q", args, name, got, want)
				}
			}
			for _, resource := range operatorResources {
				if slices.Contains(form.resources, resource) {
					continue
				}
				extra := append(slices.Clone(args), "--"+resource, "x")
				got, want := parse(extra...), usage
				if accepts(slices.Sorted(slices.Values(append(slices.Clone(form.resources), resource)))) {
					want = ""
				}
				if got != want {
					t.Errorf("%q = %q; want %q", extra, got, want)
				}
			}
			for _, resource := range form.resources {
				if got := parse(append(slices.Clone(args), "--"+resource, "again")...); got != usage {
					t.Errorf("%q with --%s repeated = %q; want its usage", args, resource, got)
				}
				empty := slices.Clone(args)
				empty[slices.Index(empty, resource+"-a")] = ""
				if got := parse(empty...); got != usage {
					t.Errorf("%q = %q; want its usage", empty, got)
				}
			}
		}
	}
	for _, verb := range []string{"add", "remove", "refresh"} {
		if got := parse(verb, "--host", "host-a"); got != operatorUsage[verb] {
			t.Errorf("%s --host alone = %q; want its usage", verb, got)
		}
	}
	for _, kind := range []string{"tenants", "pools", "receivers"} {
		if got := parse("list", kind, "--tenant", "tenant-a"); got != operatorUsage["list"] {
			t.Errorf("list %s --tenant = %q; want its usage", kind, got)
		}
	}
	if got := parse("list", "--tenant", "tenant-a"); got != operatorUsage["list"] {
		t.Errorf("list --tenant without a kind = %q; want its usage", got)
	}
}

// Every flag is a long flag: -f is undefined everywhere it was taken, and --wait is submit's alone, so the operator
// has no --wait and --timeout belongs to the forms that wait.
func TestShortFlagsAreGone(t *testing.T) {
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"operator", "apply", "--pool", "pool-a", "-f", "pool.yaml"}, "flag provided but not defined: -f"},
		{[]string{"operator", "apply", "--receiver", "receiver-a", "-f", "receiver.yaml"}, "flag provided but not defined: -f"},
		{[]string{"host", "logs", "run-a", "-f"}, "flag provided but not defined: -f"},
		{[]string{"logs", "run-a", "-f"}, "flag provided but not defined: -f"},
		{[]string{"operator", "remove", "--receiver", "receiver-a", "--wait", "1s"}, "flag provided but not defined: -wait"},
		{[]string{"operator", "remove", "--host", "host-a", "--tenant", "tenant-a", "--wait", "1s"}, "flag provided but not defined: -wait"},
		{[]string{"operator", "remove", "--tenant", "tenant-a", "--timeout", "1s"}, "unknown option --timeout"},
	} {
		var stdout, stderr bytes.Buffer
		if err := run(test.args, &stdout, &stderr); err == nil || err.Error() != test.want {
			t.Errorf("wherry %s = %v; want %q", strings.Join(test.args, " "), err, test.want)
		}
	}
}
