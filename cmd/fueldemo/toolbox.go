package main

import (
	"context"
	"errors"

	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

// newToolboxRunner will build fueldemo's toolbox-mode runner: a go-sdk
// wallet that funds itself against a live network, drives FuelKeeper's
// public API the way a real integrator would (createAction over reserved
// fuel, BEEF/AtomicBEEF, internalizeAction), and persists its own UTXO state
// in --storage. Plan 6 Task 5 replaces this stub with that implementation;
// for now toolbox mode is not runnable.
func newToolboxRunner(_ context.Context, _ options, _ *client.Client, _ *report) error {
	return errors.New("toolbox mode: not implemented yet")
}
