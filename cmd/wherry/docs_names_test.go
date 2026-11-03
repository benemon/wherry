package main

import (
	"context"
	json "encoding/json/v2"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	busclient "github.com/benemon/wherry/internal/client"
)

func docRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func docSection(t *testing.T, doc, heading string) string {
	t.Helper()
	_, section, ok := strings.Cut(doc, "\n"+heading+"\n")
	if !ok {
		t.Fatalf("missing section %s", heading)
	}
	section, _, _ = strings.Cut(section, "\n#")
	return section
}

func docNames(s string) []string {
	var names []string
	for _, m := range regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(s, -1) {
		names = append(names, m[1])
	}
	return names
}

func docTable(section string) [][]string {
	var rows [][]string
	for _, line := range strings.Split(section, "\n") {
		if strings.HasPrefix(line, "| `") {
			rows = append(rows, strings.Split(line, "|"))
		}
	}
	return rows
}

func docEqual(t *testing.T, label string, got, want []string) {
	t.Helper()
	got = slices.Sorted(slices.Values(got))
	want = slices.Sorted(slices.Values(want))
	if !slices.Equal(got, want) {
		t.Errorf("%s: docs %v; code %v", label, got, want)
	}
}

func docAST(t *testing.T, path string) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func docLiteral(e ast.Expr) string {
	if b, ok := e.(*ast.BasicLit); ok && b.Kind == token.STRING {
		s, _ := strconv.Unquote(b.Value)
		return s
	}
	return ""
}

func docTags(typ reflect.Type, tag string) []string {
	var names []string
	for i := 0; i < typ.NumField(); i++ {
		name := strings.Split(typ.Field(i).Tag.Get(tag), ",")[0]
		if name != "" && name != "-" {
			names = append(names, name)
		}
	}
	return names
}

func docTools(t *testing.T) map[string][]string {
	t.Helper()
	session := mcpSession(t, func() (*busclient.Host, error) { t.Fatal("listing tools must not connect to the bus"); return nil, nil }, nil)
	result, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	tools := map[string][]string{}
	for _, tool := range result.Tools {
		encoded, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		var schema struct {
			Properties map[string]any `json:"properties"`
		}
		if err := json.Unmarshal(encoded, &schema); err != nil {
			t.Fatal(err)
		}
		tools[tool.Name] = nil
		for name := range schema.Properties {
			tools[tool.Name] = append(tools[tool.Name], name)
		}
	}
	return tools
}

func TestMCPDocumentedNames(t *testing.T) {
	doc := docRead(t, "../../docs/mcp.md")
	rows := docTable(strings.Split(doc, "\n- ")[0])
	documented := map[string][]string{}
	for _, row := range rows {
		documented[docNames(row[1])[0]] = docNames(row[2])
	}
	var registered []string
	for name, fields := range docTools(t) {
		registered = append(registered, name)
		docEqual(t, name+" inputs", documented[name], fields)
	}
	var codes []string
	ast.Inspect(docAST(t, "mcp.go"), func(n ast.Node) bool {
		if kv, ok := n.(*ast.KeyValueExpr); ok {
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Code" {
				codes = append(codes, docLiteral(kv.Value))
			}
		}
		return true
	})
	var tools []string
	for name := range documented {
		tools = append(tools, name)
	}
	docEqual(t, "tools", tools, registered)
	var listed []string
	for _, row := range docTable(docSection(t, doc, "## Errors")) {
		listed = append(listed, docNames(row[1])...)
	}
	docEqual(t, "error codes", listed, slices.Compact(slices.Sorted(slices.Values(codes))))
}

func TestAgentDocumentedNames(t *testing.T) {
	var flags []string
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		ast.Inspect(docAST(t, path), func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "String", "Bool", "Int", "Int64", "Duration", "Func", "BoolFunc":
				if len(call.Args) > 0 {
					flags = append(flags, docLiteral(call.Args[0]))
				}
			case "StringVar", "BoolVar", "IntVar", "Int64Var", "DurationVar", "Var":
				if len(call.Args) > 1 {
					flags = append(flags, docLiteral(call.Args[1]))
				}
			}
			return true
		})
	}
	var commands []string
	for _, line := range strings.Split(usage, "\n") {
		words := strings.Fields(strings.ReplaceAll(line, "key rotate", "key~rotate"))
		if len(words) == 0 || words[0] != "wherry" {
			continue
		}
		expanded := []string{""}
		for _, word := range words[1:] {
			if strings.ContainsAny(word, "[<:") {
				break
			}
			var next []string
			for _, prefix := range expanded {
				for _, alternative := range strings.Split(word, "|") {
					next = append(next, strings.TrimSpace(prefix+" "+strings.ReplaceAll(alternative, "~", " ")))
				}
			}
			expanded = next
		}
		commands = append(commands, expanded...)
		commands = append(commands, words[1])
	}
	var tools []string
	for name := range docTools(t) {
		tools = append(tools, name)
	}
	for _, path := range []string{"../../plugins/wherry/skills/wherry/SKILL.md", "../../plugins/AGENTS.snippet.md"} {
		doc := docRead(t, path)
		for _, m := range regexp.MustCompile(`--([a-z][a-z-]+)|(?:\s|`+"`"+`)-([a-z])\b`).FindAllStringSubmatch(doc, -1) {
			name := m[1] + m[2]
			if !slices.Contains(flags, name) {
				t.Errorf("%s: undefined flag %s", path, name)
			}
		}
		for _, name := range docNames(doc) {
			if strings.HasPrefix(name, "wherry ") {
				words := strings.Fields(name)[1:]
				var command []string
				for _, word := range words {
					if strings.HasPrefix(word, "-") || strings.ContainsAny(word, "<>") || strings.ToUpper(word) == word {
						break
					}
					command = append(command, word)
				}
				if !slices.Contains(commands, strings.Join(command, " ")) {
					t.Errorf("%s: command %q absent from usage", path, name)
				}
			}
		}
		if _, list, ok := strings.Cut(doc, "same: "); ok {
			list, _, _ = strings.Cut(list, ".")
			for _, name := range docNames(list) {
				if !slices.Contains(tools, name) {
					t.Errorf("%s: unregistered tool %s", path, name)
				}
			}
		}
		for _, m := range regexp.MustCompile("(?i)(?:call )`([a-z]+)`(?: tool)?|`([a-z]+)`(?: tool| returns| refuses| first| waits)").FindAllStringSubmatch(doc, -1) {
			name := m[1] + m[2]
			if !slices.Contains(tools, name) {
				t.Errorf("%s: unregistered tool %s", path, name)
			}
		}
	}
}

func TestConfigurationDocumentedFields(t *testing.T) {
	doc := docRead(t, "../../docs/configuration.md")
	for _, tc := range []struct {
		heading string
		value   any
	}{
		{"## Client configuration", busclient.Config{}},
		{"## Receiver local configuration", receiverLocalConfig{}},
	} {
		var fields []string
		for _, row := range docTable(docSection(t, doc, tc.heading)) {
			fields = append(fields, docNames(row[1])...)
		}
		docEqual(t, tc.heading, fields, docTags(reflect.TypeOf(tc.value), "yaml"))
	}
	var variables []string
	for _, row := range docTable(docSection(t, doc, "## Environment variables")) {
		variables = append(variables, docNames(row[1])...)
	}
	var reads []string
	for _, path := range []string{"command.go", "dev.go", "../../internal/client/config.go"} {
		ast.Inspect(docAST(t, path), func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			function := ""
			switch f := call.Fun.(type) {
			case *ast.Ident:
				function = f.Name
			case *ast.SelectorExpr:
				function = f.Sel.Name
			}
			if function == "Getenv" || function == "LookupEnv" || function == "xdgDir" {
				if name := docLiteral(call.Args[0]); name != "" {
					reads = append(reads, name)
				}
			}
			return true
		})
	}
	docEqual(t, "environment variables", variables, slices.Compact(slices.Sorted(slices.Values(reads))))
}
