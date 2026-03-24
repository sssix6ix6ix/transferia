package throttler

type Throttler interface {
	ExceededLimits() bool
	AddInflight(size uint64)
	ReduceInflight(size uint64)
	InflightBytes() uint64
}
