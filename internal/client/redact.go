package client

import (
	"strings"
	"sync"

	"github.com/benemon/wherry/internal/redact"
)

// held are the forms of every credential value this process resolved; seen keys each registration's name, value and
// kind, so a value resolved again adds no forms and a short value hold left is still registered when holdStorageKey
// sees it.
var held struct {
	sync.Mutex
	forms []redact.Form
	seen  map[string]bool
}

// Registration kinds, which key held.seen beside the name and value.
const (
	heldForms = "forms"
	heldExact = "exact"
)

// hold has Redact replace value, which credential name names, for the life of the process. A value shorter than the
// redactor's minimum matches ordinary text and is left, as for every credential a run receives.
func hold(name string, value []byte) {
	remember(heldForms, name, value, func(forms []redact.Form, values map[string][]byte) []redact.Form {
		return redact.AddForms(forms, nil, values, func(string) {})
	})
}

// holdStorageKey has Redact replace value, the storage key name names, for the life of the process whatever its
// length: every storage key a host accepts is secret, so the short-value rule of hold does not apply to the two. It
// registers the value exactly whatever hold registered for the same name and value before.
func holdStorageKey(name string, value []byte) {
	remember(heldExact, name, value, func(forms []redact.Form, values map[string][]byte) []redact.Form {
		return redact.AddExact(forms, nil, values)
	})
}

// remember adds the forms of value under name once per kind, through add.
func remember(kind, name string, value []byte, add func([]redact.Form, map[string][]byte) []redact.Form) {
	held.Lock()
	defer held.Unlock()
	if key := kind + "\x00" + name + "\x00" + string(value); !held.seen[key] {
		if held.seen == nil {
			held.seen = map[string]bool{}
		}
		held.seen[key] = true
		held.forms = add(held.forms, map[string][]byte{name: value})
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
