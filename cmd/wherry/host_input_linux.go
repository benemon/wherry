package main

import "golang.org/x/sys/unix"

const hostGetTermios = unix.TCGETS
const hostSetTermios = unix.TCSETS
