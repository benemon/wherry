// Command update refreshes the credential catalog's models.dev snapshot. It fetches https://models.dev/api.json, drops
// each provider's models, and rewrites models.dev.json and SOURCE in the catalog directory, only once the whole response
// is read and decoded; a failed fetch, decode or write leaves both files as they were. The output is deterministic: the
// same response bytes give the same models.dev.json. Run it from the repository root as go run ./internal/catalog/update
// and commit the result.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/benemon/wherry/internal/catalog"
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
	entries, err := catalog.Strip(data)
	if err != nil {
		return fmt.Errorf("%s: %w", url, err)
	}
	encoded, err := catalog.Encode(entries)
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

// writeFiles writes each file into dir through a temporary file in dir, renaming them into place only once every one
// is written, so a failed write leaves the committed files unchanged.
func writeFiles(dir string, files map[string][]byte) (err error) {
	var temporary []string
	defer func() {
		if err != nil {
			for _, path := range temporary {
				os.Remove(path)
			}
		}
	}()
	renames := map[string]string{}
	for name, data := range files {
		file, err := os.CreateTemp(dir, "."+name+".*")
		if err != nil {
			return err
		}
		temporary = append(temporary, file.Name())
		if _, err := file.Write(data); err != nil {
			return errors.Join(err, file.Close())
		}
		// CreateTemp opens the file mode 0600; the committed files are readable by everyone.
		if err := errors.Join(file.Chmod(0o644), file.Sync(), file.Close()); err != nil {
			return err
		}
		renames[file.Name()] = filepath.Join(dir, name)
	}
	for from, to := range renames {
		if err := os.Rename(from, to); err != nil {
			return err
		}
	}
	return nil
}
