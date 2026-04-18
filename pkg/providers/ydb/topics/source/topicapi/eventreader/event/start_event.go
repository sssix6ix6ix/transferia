package event

import "github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"

type StartEvent struct {
	PartitionInfo PartitionInfo
	confirm       func()
}

func (e *StartEvent) Confirm() {
	e.confirm()
}

func (e *StartEvent) isEvent() {}

func NewStartEvent(event *topiclistener.EventStartPartitionSession) *StartEvent {
	return &StartEvent{
		PartitionInfo: PartitionInfo{
			TopicPath:   event.PartitionSession.TopicPath,
			PartitionID: event.PartitionSession.PartitionID,
			LastOffset:  event.CommittedOffset,
		},
		confirm: event.Confirm,
	}
}
