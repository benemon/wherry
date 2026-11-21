// Package snapshot reads and writes the credential catalog's files in their committed form, from the standard library
// alone, so the updater can build on it; the catalog package validates ids and variables.
package snapshot

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Encode writes entries as a catalog file: the ids sorted, each entry's keys sorted, indented by two spaces, with no
// HTML escaping and a final line feed, so that two writes of the same data are the same bytes. Entries are the raw
// objects, so fields wherry does not read are kept.
func Encode(entries map[string]map[string]any) ([]byte, error) {
	if entries == nil {
		return nil, errors.New("catalog: no entries")
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(entries); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Strip decodes a models.dev api.json, or a catalog file, into its entries with each one's models removed. Numbers
// keep their written form.
func Strip(data []byte) (map[string]map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var entries map[string]map[string]any
	if err := decoder.Decode(&entries); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, errors.New("trailing data after the JSON object")
	}
	for _, entry := range entries {
		delete(entry, "models")
	}
	return entries, nil
}
