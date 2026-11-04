package main

import (
	"context"
	json "encoding/json/v2"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
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
	section, _, _ = strings.Cut(section, "\n## ")
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

func docTags(typ reflect.Type) []string {
	var names []string
	for i := 0; i < typ.NumField(); i++ {
		name := strings.Split(typ.Field(i).Tag.Get("yaml"), ",")[0]
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
	_, list, ok := strings.Cut(docRead(t, "../../plugins/AGENTS.snippet.md"), "its tools do the\nsame: ")
	if !ok {
		t.Fatal("AGENTS.snippet.md lists no MCP tools")
	}
	list, _, _ = strings.Cut(list, ".")
	var tools []string
	for name := range docTools(t) {
		tools = append(tools, name)
	}
	docEqual(t, "AGENTS.snippet.md tools", docNames(list), tools)
}

func TestConfigurationDocumentedFields(t *testing.T) {
	for _, tc := range []struct {
		page, heading string
		value         any
	}{
		{"configuration-host.md", "## Client configuration", busclient.Config{}},
		{"configuration-receiver.md", "## Receiver local configuration", receiverLocalConfig{}},
	} {
		var fields []string
		for _, row := range docTable(docSection(t, docRead(t, "../../docs/"+tc.page), tc.heading)) {
			fields = append(fields, docNames(row[1])...)
		}
		docEqual(t, tc.heading, fields, docTags(reflect.TypeOf(tc.value)))
	}
	var variables []string
	for _, table := range []struct{ page, heading string }{
		{"configuration-host.md", "## Environment variables"},
		{"configuration-development.md", "## Which environment variables apply"},
	} {
		for _, row := range docTable(docSection(t, docRead(t, "../../docs/"+table.page), table.heading)) {
			variables = append(variables, docNames(row[1])...)
		}
	}
	variables = slices.Compact(slices.Sorted(slices.Values(variables)))
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
