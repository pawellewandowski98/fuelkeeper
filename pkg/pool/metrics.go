package pool

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	reserveRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_reserve_requests_total", Help: "Reserve requests by outcome.",
	}, []string{"pool", "result"})
	reserveOutputs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_reserve_outputs_total", Help: "Fuel outputs handed out.",
	}, []string{"pool"})
	reserveLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fuelkeeper_reserve_latency_seconds", Help: "Reserve latency.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"pool"})
	casMissed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_cas_missed_total", Help: "Rows lost to a concurrent reservation (duplicate-owner alarm).",
	}, []string{"pool"})
	releaseTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_release_total", Help: "Release calls by outcome.",
	}, []string{"pool", "result"})
	spendTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_spend_total", Help: "Spend calls by outcome.",
	}, []string{"pool", "result"})
	allocatorAvailable = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_allocator_available", Help: "Rows queued in memory.",
	}, []string{"pool"})
	shardsOwned = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_shards_owned", Help: "Shards leased by this instance.",
	}, []string{"pool"})
)
