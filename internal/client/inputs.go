package client

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/benemon/wherry/internal/wire"
)

// InputFile is a local file submit delivers as the input Name. Open opens it once every input's name has passed its
// checks, so a refused submission reads no file; submit closes what it opens.
type InputFile struct {
	Name string
	Open func() (io.ReadSeekCloser, error)
}

// sendFile is a file submit opens once the names are checked: the caller's, or a continued run's output with the
// record its bytes must match.
type sendFile struct {
	InputFile
	recorded *collectedOutput
}

// checkInputNames refuses, before any input file is opened, more parts and S3 inputs than a run carries, the prompt
// counted, and an input name that is not a relative path of portable file names, repeats another, is a directory of
// another or, among the caller's own, the first callers of files with every part and S3 input, lies under previous/.
func checkInputNames(parts []wire.Part, files []sendFile, s3 []S3File, callers int) error {
	if n := 1 + len(parts) + len(files) + len(s3); n > wire.MaxParts {
		return fmt.Errorf("a run carries at most %d parts and S3 inputs, the prompt included, not %d", wire.MaxParts, n)
	}
	var names []string
	for _, part := range parts {
		if part.Raw != nil || part.Data != nil {
			names = append(names, wire.InputName(part))
		}
	}
	for _, file := range files[:callers] {
		names = append(names, file.Name)
	}
	for _, file := range s3 {
		names = append(names, file.Name)
	}
	for _, name := range names {
		if strings.HasPrefix(name, previousPrefix) {
			return fmt.Errorf("input %s: names under %s are reserved for a continued run's outputs", name, previousPrefix)
		}
	}
	for _, file := range files[callers:] {
		names = append(names, file.Name)
	}
	return wire.ValidateInputNames(names)
}

// DirBase is the name the files beneath the input directory dir are delivered under: its own name with a trailing
// slash removed, the working directory's for ".", and none for the root directory.
func DirBase(dir string) (string, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	base := filepath.Base(absolute)
	if base == string(filepath.Separator) {
		return "", errors.New("the root directory has no name to deliver its files under")
	}
	return base, nil
}

// DirInputs walks root, an input directory, and returns an InputFile for each file beneath it, named base/<path
// relative to root> in lexical order, with the count of hidden entries skipped: files and directories named with a
// leading dot, which no input name carries. A symbolic link, device, socket or FIFO is refused naming it, and so is a
// directory with no file to send. Each file opens through root without blocking, and the open refuses what is not a
// regular file by then; check, when given, then sees the file's path relative to root and its stat.
func DirInputs(root *os.Root, base string, check func(relative string, info fs.FileInfo) error) ([]InputFile, int, error) {
	var files []InputFile
	skipped := 0
	err := fs.WalkDir(root.FS(), ".", func(relative string, entry fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("%s/%s: %w", base, relative, bare(err))
		}
		if relative == "." {
			return nil
		}
		if strings.HasPrefix(entry.Name(), ".") {
			skipped++
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type() != 0 {
			return fmt.Errorf("%s/%s is not a regular file", base, relative)
		}
		files = append(files, InputFile{Name: base + "/" + relative, Open: func() (io.ReadSeekCloser, error) {
			return openInRoot(root, relative, check)
		}})
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if len(files) == 0 {
		return nil, skipped, errors.New("no file to send beneath it")
	}
	return files, skipped, nil
}

// openInRoot opens relative in root as a regular file: O_NONBLOCK keeps a FIFO swapped in since the walk from blocking
// the open, and the stat refuses it.
func openInRoot(root *os.Root, relative string, check func(string, fs.FileInfo) error) (io.ReadSeekCloser, error) {
	file, err := root.OpenFile(relative, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, bare(err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, bare(err)
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, errors.New("not a regular file")
	}
	if check != nil {
		if err := check(relative, info); err != nil {
			file.Close()
			return nil, err
		}
	}
	return file, nil
}

// bare is err without the path a *fs.PathError carries, which names the root's own directory.
func bare(err error) error {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err
	}
	return err
}
