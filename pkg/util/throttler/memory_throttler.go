package throttler

import "sync"

type MemoryThrottler struct {
	BufferSize    uint64 // 0 means turned-off
	inflightMutex sync.RWMutex
	inflightBytes uint64
}

func (t *MemoryThrottler) ExceededLimits() bool {
	t.inflightMutex.RLock()
	defer t.inflightMutex.RUnlock()
	return t.inflightBytes >= t.BufferSize && t.BufferSize != 0
}

func (t *MemoryThrottler) AddInflight(size uint64) {
	t.inflightMutex.Lock()
	defer t.inflightMutex.Unlock()
	t.inflightBytes += size
}

func (t *MemoryThrottler) ReduceInflight(size uint64) {
	t.inflightMutex.Lock()
	defer t.inflightMutex.Unlock()
	t.inflightBytes = t.inflightBytes - size
}

func (t *MemoryThrottler) InflightBytes() uint64 {
	t.inflightMutex.RLock()
	defer t.inflightMutex.RUnlock()
	return t.inflightBytes
}

func NewMemoryThrottler(bufferSize uint64) Throttler {
	return &MemoryThrottler{
		BufferSize:    bufferSize,
		inflightMutex: sync.RWMutex{},
		inflightBytes: 0,
	}
}
