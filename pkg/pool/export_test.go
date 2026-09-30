package pool

// Metric vars exposed to the external test package (see metrics.go for what
// each one measures).
var (
	CoalescerBatchSize    = coalescerBatchSize
	CoalescerFlushLatency = coalescerFlushLatency
	CoalescerInflightOps  = coalescerInflightOps
	CASUnknown            = casUnknown
	CompensatingRelease   = compensatingRelease
	ShardAdoptions        = shardAdoptions
	ShardDrops            = shardDrops
	LeaseRenewFailures    = leaseRenewFailures
	StoreOpLatency        = storeOpLatency
)
