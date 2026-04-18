package event

import (
	"context"
	"io"

	"github.com/transferia/transferia/pkg/parsers"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
)

type ReadEvent struct {
	Batch parsers.MessageBatch

	commit func(ctx context.Context) error
}

func (e *ReadEvent) Commit(ctx context.Context) error {
	return e.commit(ctx)
}

func (e *ReadEvent) isEvent() {}

func NewReadEvent(event *topiclistener.ReadMessages) (*ReadEvent, error) {
	messages := make([]parsers.Message, 0, len(event.Batch.Messages))
	for _, msg := range event.Batch.Messages {
		value, err := io.ReadAll(msg)
		if err != nil {
			return nil, err
		}

		var metadata map[string]string
		if msg.Metadata != nil {
			metadata = make(map[string]string, len(msg.Metadata))
			for k, v := range msg.Metadata {
				metadata[k] = string(v)
			}
		}

		messages = append(messages, parsers.Message{
			Offset:     uint64(msg.Offset),
			Key:        []byte(msg.ProducerID),
			Value:      value,
			CreateTime: msg.CreatedAt,
			WriteTime:  msg.CreatedAt,
			Headers:    metadata,
			SeqNo:      uint64(msg.SeqNo),
		})
	}

	return &ReadEvent{
		Batch: parsers.MessageBatch{
			Topic:     event.Batch.Topic(),
			Partition: uint32(event.PartitionSession.PartitionID),
			Messages:  messages,
		},
		commit: event.ConfirmWithAck,
	}, nil
}
