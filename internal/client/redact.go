package client

import (
	"strings"
	"sync"

	"github.com/benemon/wherry/internal/redact"
)

// held are the forms of every credential value this process resolved, each with the marker of its name under no key;
// seen keys the name and value of each, so a value resolved again adds no forms.
var held struct {
	sync.Mutex
	forms []redact.Form
	seen  map[string]bool
}

// hold has Redact replace value, which credential name names, for the life of the process.
func hold(name string, value []byte) {
	held.Lock()
	defer held.Unlock()
	if key := name + "\x00" + string(value); !held.seen[key] {
		if held.seen == nil {
			held.seen = map[string]bool{}
		}
		held.seen[key] = true
		held.forms = redact.AddForms(held.forms, nil, map[string][]byte{name: value}, func(string) {})
	}
}

// Redact is text with every form of every credential value this process resolved replaced by its marker.
func Redact(text string) string {
	held.Lock()
	defer held.Unlock()
	var out strings.Builder
	redact.To(&out, []byte(text), held.forms)
	return out.String()
}
