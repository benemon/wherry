// Command update refreshes the credential catalog's models.dev snapshot. It fetches https://models.dev/api.json, drops
// each provider's models, and rewrites models.dev.json and SOURCE in the catalog directory, only once the whole response
// is read and decoded; a failed fetch, decode or write leaves both files as they were, and the two change together or
// not at all. The output is deterministic: the same response bytes give the same models.dev.json. Run it from the
// repository root as go run ./internal/catalog/update and commit the result. It builds from the standard library and
// the catalog's snapshot package alone.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/catalog/snapshot"
)

const defaultURL = "https://models.dev/api.json"

// responseLimit bounds the response read: api.json is about 7 MB.
const responseLimit = 64 << 20

func main() {
	url := flag.String("url", defaultURL, "the models.dev api.json to fetch")
	dir := flag.String("dir", filepath.Join("internal", "catalog"), "the catalog directory holding models.dev.json and SOURCE")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := update(ctx, http.DefaultClient, *url, *dir, time.Now()); err != nil {
		fmt.Fprintln(os.Stderr, "update:", err)
		os.Exit(1)
	}
	fmt.Println("updated", filepath.Join(*dir, "models.dev.json"), "and", filepath.Join(*dir, "SOURCE"))
}

// update fetches url with client, strips the providers' models and writes dir's models.dev.json and SOURCE, which
// records url, now in UTC and the SHA-256 of the response bytes as fetched.
func update(ctx context.Context, client *http.Client, url, dir string, now time.Time) error {
	data, err := fetch(ctx, client, url)
	if err != nil {
		return err
	}
	entries, err := snapshot.Strip(data)
	if err != nil {
		return fmt.Errorf("%s: %w", url, err)
	}
	encoded, err := snapshot.Encode(entries)
	if err != nil {
		return err
	}
	source := fmt.Sprintf("url: %s\nfetched: %s\nsha256: %x\n", url, now.UTC().Format(time.RFC3339), sha256.Sum256(data))
	return writeFiles(dir, map[string][]byte{"models.dev.json": encoded, "SOURCE": []byte(source)})
}

// fetch is the whole body of a 200 response to a GET of url.
func fetch(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", url, response.Status)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, responseLimit+1))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", url, err)
	}
	if len(data) > responseLimit {
		return nil, fmt.Errorf("%s: response exceeds %d bytes", url, responseLimit)
	}
	return data, nil
}

// rename is a variable so tests can fail a rename after the first one succeeded.
var rename = os.Rename

// writeFiles writes each file into dir through a temporary file in dir, then commits them all or none: a hard link keeps
// each existing file under a backup name, the temporary files are renamed into place one by one, and a rename that
// fails has every file renamed so far put back from its backup, so a failure at any point leaves the committed files as
// they were. A target that exists and is not a regular file is refused before anything is renamed. Nothing temporary
// is left behind.
func writeFiles(dir string, files map[string][]byte) (err error) {
	var temporary []string
	defer func() {
		for _, path := range temporary {
			os.Remove(path)
		}
	}()
	names := slices.Sorted(maps.Keys(files))
	targets := map[string]string{}
	for _, name := range names {
		target := filepath.Join(dir, name)
		if info, statErr := os.Lstat(target); statErr == nil && !info.Mode().IsRegular() {
			return fmt.Errorf("%s exists and is not a regular file", target)
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		file, err := os.CreateTemp(dir, "."+name+".*")
		if err != nil {
			return err
		}
		temporary = append(temporary, file.Name())
		if _, err := file.Write(files[name]); err != nil {
			return errors.Join(err, file.Close())
		}
		// CreateTemp opens the file mode 0600; the committed files are readable by everyone.
		if err := errors.Join(file.Chmod(0o644), file.Sync(), file.Close()); err != nil {
			return err
		}
		targets[name] = file.Name()
	}
	// Each existing file is kept under a second name before any is replaced; the link shares the file, so nothing is
	// copied and the original is whole whatever happens to the name it was committed under.
	backups := map[string]string{}
	for _, name := range names {
		target := filepath.Join(dir, name)
		if _, statErr := os.Lstat(target); errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		// A backup left by an interrupted run is stale: the committed file is the one that counts.
		backup := filepath.Join(dir, "."+name+".backup")
		if err := os.Remove(backup); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.Link(target, backup); err != nil {
			return err
		}
		temporary = append(temporary, backup)
		backups[name] = backup
	}
	var committed []string
	for _, name := range names {
		if err := rename(targets[name], filepath.Join(dir, name)); err != nil {
			var restore []error
			for _, done := range committed {
				target := filepath.Join(dir, done)
				if backup, existed := backups[done]; existed {
					restore = append(restore, os.Rename(backup, target))
				} else {
					restore = append(restore, os.Remove(target))
				}
			}
			return errors.Join(append([]error{err}, restore...)...)
		}
		committed = append(committed, name)
	}
	return nil
}
