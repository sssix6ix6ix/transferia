package event

import "github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"

type StopEvent struct {
	PartitionInfo PartitionInfo
	confirm       func()
}

func (e *StopEvent) Confirm() {
	e.confirm()
}

func (e *StopEvent) isEvent() {}

func NewStopEvent(event *topiclistener.EventStopPartitionSession) *StopEvent {
	return &StopEvent{
		PartitionInfo: PartitionInfo{
			TopicPath:   event.PartitionSession.TopicPath,
			PartitionID: event.PartitionSession.PartitionID,
			LastOffset:  event.CommittedOffset,
		},
		confirm: event.Confirm,
	}
}
