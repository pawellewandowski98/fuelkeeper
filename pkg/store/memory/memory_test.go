package memory_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.RunSuite(t, func(_ *testing.T, now func() time.Time) store.Store {
		return memory.New(memory.WithClock(now))
	})
}
