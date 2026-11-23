package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/view"
)

// operatorView runs the operator network view: a rebuild of the window, then live tailing, served on loopback until
// an interrupt. It reads as wherry.operator and never produces.
func operatorView(ctx context.Context, service *operator.Service, options map[string]string, stdout, stderr io.Writer) error {
	window := view.DefaultWindow
	if value, given := options["window"]; given {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return errors.New("--window must be a positive duration, such as 24h")
		}
		window = parsed
	}
	port := 0
	if value, given := options["port"]; given {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 || parsed > 65535 {
			return errors.New("--port must be 0 to 65535")
		}
		port = parsed
	}
	logger := log.New(stderr, "wherry: ", log.LstdFlags|log.LUTC)
	v := view.New(service.Operator, view.Options{Window: window, Logger: logger})
	defer v.Close()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	started := time.Now()
	fmt.Fprintf(stderr, "wherry: rebuilding the last %s\n", window)
	if err := v.Rebuild(ctx); err != nil {
		return fmt.Errorf("rebuild: %w", err)
	}
	snapshot := v.Snapshot(1)
	fmt.Fprintf(stderr, "wherry: rebuilt %d runs in %s\n", snapshot.RunsTotal, time.Since(started).Truncate(time.Millisecond))
	server, err := view.Serve(v, view.ServeOptions{Port: port, ExternalURL: options["external-url"], Logger: logger})
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, server.URL())
	err = v.Run(ctx)
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Close(closeCtx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
