package throttler

type StubThrottler struct{}

func (t *StubThrottler) ExceededLimits() bool {
	return false
}

func (t *StubThrottler) AddInflight(_ uint64) {}

func (t *StubThrottler) ReduceInflight(_ uint64) {}

func (t *StubThrottler) InflightBytes() uint64 {
	return 0
}

func NewStubThrottler() Throttler {
	return &StubThrottler{}
}
