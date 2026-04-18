package eventreader

import (
	"context"

	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader/event"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"go.ytsaurus.tech/library/go/core/log"
)

type eventWithError struct {
	event event.Event
	err   error
}

type customEventHandler struct {
	topiclistener.BaseHandler

	eventsCh chan<- eventWithError

	logger log.Logger
}

func (h *customEventHandler) OnStartPartitionSessionRequest(ctx context.Context, listenerEvent *topiclistener.EventStartPartitionSession) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.eventsCh <- eventWithError{event.NewStartEvent(listenerEvent), nil}:
		h.logger.Debug("listen start reading partition",
			log.String("topic", listenerEvent.PartitionSession.TopicPath),
			log.Int64("partition", listenerEvent.PartitionSession.PartitionID),
		)
		return nil
	}
}

func (h *customEventHandler) OnReadMessages(ctx context.Context, listenerEvent *topiclistener.ReadMessages) error {
	readEvent, err := event.NewReadEvent(listenerEvent)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.eventsCh <- eventWithError{readEvent, err}:
		h.logger.Debug("listen messages",
			log.String("topic", listenerEvent.PartitionSession.TopicPath),
			log.Int64("partition", listenerEvent.PartitionSession.PartitionID),
			log.Int("message_count", len(listenerEvent.Batch.Messages)),
		)
		return nil
	}
}

func (h *customEventHandler) OnStopPartitionSessionRequest(ctx context.Context, listenerEvent *topiclistener.EventStopPartitionSession) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.eventsCh <- eventWithError{event.NewStopEvent(listenerEvent), nil}:
		h.logger.Debug("listen stop reading partition", log.Bool("graceful", listenerEvent.Graceful))
		return nil
	}
}

func newEventHandler(eventsCh chan eventWithError, logger log.Logger) topiclistener.EventHandler {
	return &customEventHandler{
		BaseHandler: topiclistener.BaseHandler{},
		eventsCh:    eventsCh,
		logger:      logger,
	}
}
