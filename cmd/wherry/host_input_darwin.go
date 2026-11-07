package main

import "golang.org/x/sys/unix"

const hostGetTermios = unix.TIOCGETA
const hostSetTermios = unix.TIOCSETA
