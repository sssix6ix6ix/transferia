package sourcehelpers

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

func WaitForItems(src abstract.Source, expectedItemsCount int, waitBeforeClose time.Duration) ([][]abstract.ChangeItem, error) {
	res := make([][]abstract.ChangeItem, 0)
	sink := mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
		res = append(res, items)
		expectedItemsCount -= len(items)
		return nil
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- src.Run(sink)
	}()

	for expectedItemsCount > 0 {
		select {
		case err := <-errCh:
			return nil, err
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	time.Sleep(waitBeforeClose)
	src.Stop()

	return res, nil
}

func WaitForItemsQueueToS3(src abstract.QueueToS3Source, expectedItemsCount int, waitBeforeClose time.Duration) ([][]abstract.ChangeItem, error) {
	res := make([][]abstract.ChangeItem, 0)
	sink := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
		return item.QueueMessageMeta.Offset
	}, func(items []abstract.ChangeItem) error {
		res = append(res, items)
		expectedItemsCount -= len(items)
		return nil
	}, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- src.Run(sink)
	}()

	for expectedItemsCount > 0 {
		select {
		case err := <-errCh:
			return nil, err
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	time.Sleep(waitBeforeClose)
	src.Stop()

	return res, nil
}
