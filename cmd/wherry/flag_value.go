package main

import (
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/operator"
)

// flagValue is the value of flag given value, as every flag that takes a value reads it: the string as given, the
// contents of FILE for @FILE, a leading ~ expanded, or standard input for @-. open opens FILE: the CLI's opens any path,
// the MCP server's confines it to the input roots. stdin reads @-; nil, as where standard input carries another
// protocol, refuses it, and so does a terminal. An error names the path, never the contents.
func flagValue(flag, value string, open func(string) (*os.File, error), stdin io.Reader) (string, error) {
	path, ok := strings.CutPrefix(value, "@")
	if !ok {
		return value, nil
	}
	switch path {
	case "":
		return "", fmt.Errorf("%s @ names no file; a value starting with @ names a file, and @- standard input", flag)
	case "-":
		if stdin == nil {
			return "", fmt.Errorf("%s @- cannot read standard input here; give the text or @FILE", flag)
		}
		if file, ok := stdin.(*os.File); ok && isTerminal(file) {
			return "", fmt.Errorf("%s @-: standard input is a terminal; pipe the value in, or give the text or @FILE", flag)
		}
		contents, err := io.ReadAll(stdin)
		if err != nil {
			return "", fmt.Errorf("%s @-: standard input: %w", flag, err)
		}
		return string(contents), nil
	}
	expanded, err := busclient.ExpandHome(path)
	if err != nil {
		return "", err
	}
	file, err := open(expanded)
	if err != nil {
		return "", fmt.Errorf("%s @%s: %w; a value starting with @ names a file", flag, path, err)
	}
	defer file.Close()
	contents, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("%s @%s: %w", flag, path, err)
	}
	return string(contents), nil
}

// openFlagFile opens the file a CLI flag's @FILE names: any path the user names.
func openFlagFile(path string) (*os.File, error) {
	return os.Open(path)
}

// caCertFlag resolves --ca-cert's value through flagValue and builds the pool it gives: the PEM text as a configuration's
// ca_cert will hold it, CRLF normalised to LF, and the roots. A value from which Go appends no certificate, a path given
// without @ among them, is refused.
func caCertFlag(value string) (string, *x509.CertPool, error) {
	text, err := flagValue("--ca-cert", value, openFlagFile, stdin)
	if err != nil {
		return "", nil, err
	}
	text = strings.ReplaceAll(text, "\r\n", "\n")
	roots, err := operator.RootPool(text)
	if errors.Is(err, operator.ErrNoCertificate) {
		return "", nil, fmt.Errorf("--ca-cert %w; give the PEM text, @FILE or @-", err)
	}
	if err != nil {
		return "", nil, err
	}
	return text, roots, nil
}
