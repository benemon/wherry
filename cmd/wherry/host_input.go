//go:build linux || darwin

package main

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

type hostInput struct {
	timed  bool
	get    func() (*unix.Termios, error)
	set    func(*unix.Termios) error
	read   func([]byte) (int, error)
	hungup func() (bool, error)
}

func newHostInput(file *os.File) *hostInput {
	fd := int(file.Fd())
	return &hostInput{
		get:  func() (*unix.Termios, error) { return unix.IoctlGetTermios(fd, hostGetTermios) },
		set:  func(state *unix.Termios) error { return unix.IoctlSetTermios(fd, hostSetTermios, state) },
		read: func(buffer []byte) (int, error) { return unix.Read(fd, buffer) },
		hungup: func() (bool, error) {
			fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			_, err := unix.Poll(fds, 0)
			return fds[0].Revents&(unix.POLLHUP|unix.POLLERR|unix.POLLNVAL) != 0, err
		},
	}
}
func (in *hostInput) Read(buffer []byte) (n int, err error) {
	// io.Reader cannot expose unix.Read's negative error count.
	defer func() { n = max(0, n) }()
	if !in.timed {
		n, err = in.read(buffer)
		if n == 0 && err == nil {
			err = io.EOF
		}
		return
	}
	state, err := in.get()
	if err != nil {
		return 0, err
	}
	next := *state
	next.Cc[unix.VMIN], next.Cc[unix.VTIME] = 0, 1
	defer func() {
		restoreErr := in.set(state)
		if restoreErr != nil {
			err = errors.Join(err, restoreErr)
			n = 0
		}
	}()
	if err = in.set(&next); err != nil {
		return 0, err
	}
	n, err = in.read(buffer)
	if n == 0 && err == nil {
		var hungup bool
		hungup, err = in.hungup()
		if err == nil {
			if hungup {
				err = io.EOF
			} else {
				err = errPickTimeout
			}
		}
	}
	return
}
