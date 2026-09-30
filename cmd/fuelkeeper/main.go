// Command fuelkeeper runs the FuelKeeper service (serve) and its admin CLI
// (pools, shards, quarantine, release) against a running instance.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// version is the build version, set via -ldflags "-X main.version=...".
var version = "dev"

// usageError marks an error as a command-line usage mistake -- a bad flag, a
// missing required flag, an unknown subcommand -- as opposed to a runtime
// failure (a server that would not start, a request that failed). main uses
// it to choose between the two conventional exit codes: 2 for usage, 1 for
// everything else.
type usageError struct{ err error }

// Error implements error.
func (e *usageError) Error() string { return e.err.Error() }

// Unwrap lets errors.Is/errors.As see through to the wrapped error.
func (e *usageError) Unwrap() error { return e.err }

// usageErrorf builds a usageError from a format string, the usual way a
// command reports a missing or malformed flag.
func usageErrorf(format string, args ...any) error {
	return &usageError{err: fmt.Errorf(format, args...)}
}

// wrapUsageError marks err (e.g. a flag.FlagSet.Parse failure) as a
// usageError; a nil err stays nil.
func wrapUsageError(err error) error {
	if err == nil {
		return nil
	}
	return &usageError{err: err}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	// A second SIGINT/SIGTERM must force an immediate exit rather than wait
	// out the graceful shutdown again: once ctx is cancelled by the first
	// signal, stop it (unregistering our handler) so the next one hits Go's
	// default disposition, which terminates the process at once.
	context.AfterFunc(ctx, stop)
	defer stop()

	var err error
	switch os.Args[1] {
	case "serve":
		err = serveCmd(ctx, os.Args[2:])
	case "pools", "shards", "quarantine", "release":
		err = adminCmd(ctx, os.Args[1], os.Args[2:])
	case "version":
		fmt.Println(version)
	case "-h", "-help", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		var uerr *usageError
		if errors.As(err, &uerr) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  fuelkeeper serve --config fuelkeeper.yaml
  fuelkeeper pools      --url http://host:8080 [--key K]
  fuelkeeper shards     --url URL --pool P [--status S] [--limit N]
  fuelkeeper quarantine --url URL --pool P list|requeue [--outpoints a:0,b:1]|retire --txid T
  fuelkeeper release    --url URL --pool P --token T --outpoints a:0,b:1
  fuelkeeper version`)
}
