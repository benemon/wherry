package main

import (
	"bytes"
	"errors"
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// dispatched are the commands the dispatchers accept: the string cases of their switches on args[0] or command and
// the strings they compare args[0] or pos[0] with, under each dispatcher's role word, and the day-to-day commands.
func dispatched(t *testing.T) []string {
	t.Helper()
	roles := map[string]string{"run": "", "runOperator": "operator ", "runReceiver": "receiver ", "runHost": "host ", "runDev": "dev "}
	files, err := parser.ParseDir(token.NewFileSet(), ".", func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	commands := map[string]bool{}
	subject := func(expr ast.Expr) bool {
		switch types.ExprString(expr) {
		case "args[0]", "pos[0]", "command":
			return true
		}
		return false
	}
	add := func(prefix string, expr ast.Expr) {
		if literal, ok := expr.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			if name, _ := strconv.Unquote(literal.Value); !strings.HasPrefix(name, "-") {
				commands[prefix+name] = true
			}
		}
	}
	for _, file := range files["main"].Files {
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			prefix, role := "", false
			if ok {
				prefix, role = roles[function.Name.Name]
			}
			if !role {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.SwitchStmt:
					if node.Tag != nil && subject(node.Tag) {
						for _, clause := range node.Body.List {
							for _, value := range clause.(*ast.CaseClause).List {
								add(prefix, value)
							}
						}
					}
				case *ast.BinaryExpr:
					if (node.Op == token.EQL || node.Op == token.NEQ) && subject(node.X) {
						add(prefix, node.Y)
					}
				}
				return true
			})
		}
	}
	for name := range dayToDay {
		commands[name], commands["host "+name] = true, true
	}
	return slices.Sorted(maps.Keys(commands))
}

// usageCommands are the commands usage names: each line's alternatives, which share the first's words but its last.
func usageCommands() map[string]bool {
	named := map[string]bool{}
	for _, line := range strings.Split(usage, "\n") {
		line, ok := strings.CutPrefix(strings.TrimSpace(line), "wherry ")
		if !ok || strings.HasPrefix(line, "<") {
			continue
		}
		var prefix []string
		for i, alternative := range strings.Split(line, "|") {
			words := strings.Fields(alternative)
			for j, word := range words {
				if strings.HasPrefix(word, "[") {
					words = words[:j]
					break
				}
			}
			if i == 0 {
				prefix = words[:len(words)-1]
				words = words[len(words)-1:]
			}
			named[strings.Join(append(slices.Clone(prefix), words...), " ")] = true
		}
	}
	return named
}

// wherry --help names every command a dispatcher accepts; one with subcommands is named by them.
func TestUsageNamesEveryDispatchedCommand(t *testing.T) {
	// dev mock is the dev environment's agent, which wherry runs itself.
	hidden := map[string]bool{"dev mock": true}
	named := usageCommands()
	commands := dispatched(t)
	for _, want := range []string{"operator init", "operator pool apply", "host key", "host submit", "receiver run", "dev up", "version"} {
		if !slices.Contains(commands, want) {
			t.Fatalf("dispatched commands %v lack %q", commands, want)
		}
	}
	for _, command := range commands {
		found := named[command]
		for name := range named {
			found = found || strings.HasPrefix(name, command+" ")
		}
		if !found && !hidden[command] {
			t.Errorf("wherry --help does not name %q", command)
		}
	}
}

// An explicit -h prints the command's usage on stderr and ends wherry with status 0, as flag.ErrHelp does in main.
func TestHelpPrintsUsage(t *testing.T) {
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"operator", "-h"}, operatorUsage[""] + "\n"},
		{[]string{"operator", "--help"}, operatorUsage[""] + "\n"},
		{[]string{"operator", "init", "-h"}, operatorUsage["init"] + "\n"},
		{[]string{"operator", "pool", "apply", "-h"}, operatorUsage["pool apply"] + "\n"},
		{[]string{"operator", "receiver", "remove", "nomad-1", "-h"}, operatorUsage["receiver remove"] + "\n"},
		{[]string{"host", "-h"}, "usage: wherry host <init|key rotate|receivers|mcp|submit|wait|status|events|logs|cancel>\n"},
		{[]string{"host", "init", "-h"}, "Usage of host init:\n"},
		{[]string{"host", "key", "rotate", "-h"}, "Usage of host key rotate:\n"},
		{[]string{"host", "receivers", "-h"}, "Usage of receivers:\n"},
		{[]string{"host", "submit", "-h"}, "Usage of submit:\n"},
		{[]string{"submit", "-h"}, "Usage of submit:\n"},
	} {
		var stdout, stderr bytes.Buffer
		err := run(test.args, &stdout, &stderr)
		if !errors.Is(err, flag.ErrHelp) || !strings.HasPrefix(stderr.String(), test.want) || stdout.Len() != 0 {
			t.Errorf("wherry %s = %v printing %q on stderr and %q on stdout; want flag.ErrHelp and stderr starting %q", strings.Join(test.args, " "), err, stderr.String(), stdout.String(), test.want)
		}
	}
}
