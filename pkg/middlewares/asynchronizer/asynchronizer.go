package asynchronizer

import (
	"github.com/transferia/transferia/pkg/abstract2"
)

// Asynchronizer is tasks.asynchronousSnapshotState for abstract2.
// However, as there is no way to tell if a batch contains "non-row" events, push is always asynchronous.
type Asynchronizer interface {
	Close() error
	Push(input abstract2.EventBatch) error
}

type EventTargetWrapper struct {
	target abstract2.EventTarget
	errChs []chan error
}

func NewEventTargetWrapper(target abstract2.EventTarget) *EventTargetWrapper {
	return &EventTargetWrapper{
		target: target,
		errChs: make([]chan error, 0),
	}
}

func (s *EventTargetWrapper) Close() error {
	for _, errCh := range s.errChs {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *EventTargetWrapper) Push(input abstract2.EventBatch) error {
	var result error = nil
	lastReadChI := 0

overErrChs:
	for _, errCh := range s.errChs {
		select {
		case err := <-errCh:
			lastReadChI += 1
			if err != nil {
				result = err
				break overErrChs
			}
		default:
			break overErrChs
		}
	}
	s.errChs = s.errChs[lastReadChI:]
	if result != nil {
		return result
	}

	s.errChs = append(s.errChs, s.target.AsyncPush(input))
	return nil
}
