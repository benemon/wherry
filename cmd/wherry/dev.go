package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

func runDev(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "mock" {
		return runDevMock(args[1:], stdout)
	}
	return errors.New("unknown dev command")
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}
