// Command fuelkeeper runs the FuelKeeper service (serve) and its admin CLI
// (pools, shards, quarantine, release) against a running instance.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// version is the build version, set via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
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
