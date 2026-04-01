package mocksink

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

type MockQueueToS3Sink struct {
	getOffsetF   func(item abstract.ChangeItem) uint64
	pushF        func(items []abstract.ChangeItem) error
	needSendResF func(offsets []uint64) bool

	accumulatedOffsets []uint64
}

func (s *MockQueueToS3Sink) AsyncV2Push(_ context.Context, errCh chan<- abstract.AsyncPushResult, items []abstract.ChangeItem) {
	if s.pushF == nil {
		return
	}

	if err := s.pushF(items); err != nil {
		errCh <- &abstract.QueueSourceAsyncPushResult{
			Result: abstract.QueueResult{},
			Err:    err,
		}
	}

	for _, item := range items {
		s.accumulatedOffsets = append(s.accumulatedOffsets, s.getOffsetF(item))
	}

	if s.needSendResF != nil && !s.needSendResF(s.accumulatedOffsets) {
		return
	}

	errCh <- &abstract.QueueSourceAsyncPushResult{
		Result: abstract.QueueResult{
			Offsets: s.accumulatedOffsets,
		},
		Err: nil,
	}
	s.accumulatedOffsets = make([]uint64, 0)
}

func (s *MockQueueToS3Sink) Close() error {
	return nil
}

func NewMockQueueToS3Sink(
	getOffsetF func(item abstract.ChangeItem) uint64,
	pushF func(items []abstract.ChangeItem) error,
	needSendResF func(offsets []uint64) bool,
) *MockQueueToS3Sink {
	return &MockQueueToS3Sink{
		getOffsetF:         getOffsetF,
		pushF:              pushF,
		needSendResF:       needSendResF,
		accumulatedOffsets: make([]uint64, 0),
	}
}
