package pool

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// latencyBuckets is shared by every latency histogram in this package so
// dashboards can compare them directly.
var latencyBuckets = []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}

// batchSizeBuckets covers a single op up to the largest batch the coalescer
// or store is ever configured to accept.
var batchSizeBuckets = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000}

var (
	reserveRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_reserve_requests_total", Help: "Reserve requests by outcome.",
	}, []string{"pool", "result"})
	reserveOutputs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_reserve_outputs_total", Help: "Fuel outputs handed out.",
	}, []string{"pool"})
	reserveLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fuelkeeper_reserve_latency_seconds", Help: "Reserve latency.",
		Buckets: latencyBuckets,
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

	coalescerBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fuelkeeper_coalescer_batch_size", Help: "Ops grouped into one coalescer flush.",
		Buckets: batchSizeBuckets,
	}, []string{"pool"})
	coalescerFlushLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fuelkeeper_coalescer_flush_latency_seconds",
		Help:    "Time from the first attempt to the final outcome, including retry backoff.",
		Buckets: latencyBuckets,
	}, []string{"pool"})
	coalescerInflightOps = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_coalescer_inflight_ops", Help: "Ops queued plus ops currently in a flush.",
	}, []string{"pool"})
	casUnknown = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_cas_unknown_total", Help: "Ops whose Reserve outcome was unknown after retries were exhausted.",
	}, []string{"pool"})
	compensatingRelease = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_compensating_release_total", Help: "Compensating Release calls issued by the coalescer after an in-doubt flush.",
	}, []string{"pool"})
	shardAdoptions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_shard_adoptions_total", Help: "AdoptOnce attempts by result: ok, none (nothing adoptable), error.",
	}, []string{"pool", "result"})
	shardDrops = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_shard_drops_total", Help: "Owned shards dropped, by reason.",
	}, []string{"pool", "reason"})
	leaseRenewFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_lease_renew_failures_total", Help: "RenewShard calls that failed transiently (excludes ErrNotOwner/ErrNotFound, which are lease loss, not failure).",
	}, []string{"pool"})
	storeOpLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fuelkeeper_store_op_latency_seconds", Help: "Store call latency observed at the pool's call sites, by operation.",
		Buckets: latencyBuckets,
	}, []string{"pool", "op"})
)

// Spend batching (spendbatch.go).
var (
	spendBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fuelkeeper_spend_batch_size", Help: "Spend requests grouped into one SpendBatch flush (one durable store write).",
		Buckets: batchSizeBuckets,
	}, []string{"pool"})
	spendFlushSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fuelkeeper_spend_flush_seconds",
		Help:    "Spend flush time from the first SpendBatch attempt to the final outcome, including retry backoff.",
		Buckets: latencyBuckets,
	}, []string{"pool"})
	spendFlushRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_spend_flush_retries_total", Help: "SpendBatch attempts retried after ErrUnavailable or a per-attempt timeout.",
	}, []string{"pool"})
	spendInflightOps = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_spend_inflight_ops", Help: "Outpoints in spend requests queued plus those currently in a spend flush.",
	}, []string{"pool"})
	spendBackpressure = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_spend_backpressure_total", Help: "Spend requests rejected because the spend batcher's in-flight outpoint limit was reached.",
	}, []string{"pool"})
	spendFallback = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_spend_fallback_total", Help: "Spend store writes made one per request, unbatched, by reason (unsupported: the store has no SpendBatch; disabled: spend_batching.enabled is false).",
	}, []string{"pool", "reason"})
)
