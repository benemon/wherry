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

// hold has Redact replace value, which credential name names, for the life of the process. A value shorter than the
// redactor's minimum matches ordinary text and is left, as for every credential a run receives.
func hold(name string, value []byte) {
	remember(name, value, func(forms []redact.Form, values map[string][]byte) []redact.Form {
		return redact.AddForms(forms, nil, values, func(string) {})
	})
}

// holdStorageKey has Redact replace value, the storage key name names, for the life of the process whatever its
// length: every storage key a host accepts is secret, so the short-value rule of hold does not apply to the two.
func holdStorageKey(name string, value []byte) {
	remember(name, value, func(forms []redact.Form, values map[string][]byte) []redact.Form {
		return redact.AddExact(forms, nil, values)
	})
}

// remember adds the forms of value under name once, through add.
func remember(name string, value []byte, add func([]redact.Form, map[string][]byte) []redact.Form) {
	held.Lock()
	defer held.Unlock()
	if key := name + "\x00" + string(value); !held.seen[key] {
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
