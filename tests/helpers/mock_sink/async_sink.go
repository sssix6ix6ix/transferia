package mocksink

import "github.com/transferia/transferia/pkg/abstract"

type MockAsyncSink struct {
	AsyncCallback func(items []abstract.ChangeItem) chan error
	PushCallback  func(items []abstract.ChangeItem) error
}

func (s MockAsyncSink) AsyncPush(items []abstract.ChangeItem) chan error {
	if s.AsyncCallback != nil {
		return s.AsyncCallback(items)
	}

	errCh := make(chan error, 1)
	errCh <- s.PushCallback(items)
	return errCh
}

func (s MockAsyncSink) Close() error {
	return nil
}

func NewMockAsyncSink(callback func([]abstract.ChangeItem) error) *MockAsyncSink {
	if callback == nil {
		callback = func([]abstract.ChangeItem) error { return nil }
	}

	return &MockAsyncSink{
		AsyncCallback: nil,
		PushCallback:  callback,
	}
}

func NewMockAsyncSinkWithChan(callback func([]abstract.ChangeItem) chan error) *MockAsyncSink {
	return &MockAsyncSink{
		AsyncCallback: callback,
		PushCallback:  nil,
	}
}
