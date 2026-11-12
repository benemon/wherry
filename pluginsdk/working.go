//go:build unix

package pluginsdk

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// ErrRefused marks a working file ReadWorkingFile refuses for what the agent made of it: a link, a FIFO, a device, a
// directory, a hard link, or a component it cannot open. A plugin reads it as nothing, not as a platform failure.
var ErrRefused = errors.New("working file refused")

// ReadWorkingFile reads name, a slash-separated path beneath the directory root, for ReadWorking: root is trusted, and
// the agent may control everything beneath it. Each component is opened from the one before without following a
// symbolic link, the file without blocking, so a FIFO cannot hold the call; anything but a regular file is refused,
// and at most MaxWorking+1 bytes are read. A missing root or component reads as nothing. A refusal the agent can cause
// wraps ErrRefused.
func ReadWorkingFile(root, name string) ([]byte, error) {
	components := strings.Split(name, "/")
	if slices.ContainsFunc(components, func(component string) bool { return component == "" || component == "." || component == ".." }) {
		return nil, fmt.Errorf("working file %q is not a path beneath its root", name)
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: root, Err: err}
	}
	for i, component := range components {
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_DIRECTORY
		if i == len(components)-1 {
			flags = unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
		}
		next, err := unix.Openat(fd, component, flags, 0)
		unix.Close(fd)
		if errors.Is(err, unix.ENOENT) {
			return nil, nil
		}
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.ENXIO) {
			err = fmt.Errorf("%w: %w", ErrRefused, err)
		}
		if err != nil {
			return nil, &fs.PathError{Op: "open", Path: strings.Join(components[:i+1], "/"), Err: err}
		}
		fd = next
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	// A hard link reaches a file outside the run. A file renamed into place has one link, and none once a later rename
	// replaces it after the open.
	if stat, ok := info.Sys().(*syscall.Stat_t); !info.Mode().IsRegular() || !ok || stat.Nlink > 1 {
		return nil, fmt.Errorf("%w: working file %s is not a regular file with at most one link", ErrRefused, name)
	}
	data, err := io.ReadAll(io.LimitReader(file, MaxWorking+1))
	if err != nil || len(data) > MaxWorking {
		return nil, err
	}
	return data, nil
}
