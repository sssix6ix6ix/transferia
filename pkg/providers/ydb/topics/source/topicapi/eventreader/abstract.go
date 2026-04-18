package eventreader

import (
	"context"

	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader/event"
)

type EventReader interface {
	NextEvent(ctx context.Context) (event.Event, error)
	Close(ctx context.Context) error
}

type OffsetCommitEventReader interface {
	EventReader
	CommitOffset(ctx context.Context, offset uint64) error
}
