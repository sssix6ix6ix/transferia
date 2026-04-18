package eventreader

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"go.ytsaurus.tech/library/go/core/log"
)

type PartitionEventReader struct {
	*TopicEventReader

	partition int64
	topicPath string
	consumer  string
	ydbClient *ydb.Driver
}

// CommitOffset commits offset + 1, since in YDB topics, the committed offset
// does not indicate the last message read, but the message that will be read next.
func (r *PartitionEventReader) CommitOffset(ctx context.Context, offset uint64) error {
	return r.ydbClient.Topic().CommitOffset(ctx, r.topicPath, r.partition, r.consumer, int64(offset)+1,
		topicoptions.WithCommitOffsetReadSessionID(r.listener.ReadSessionID()),
	)
}

func NewPartitionEventReader(consumer string, topicPath string, partition int64, ydbClient *ydb.Driver, logger log.Logger) (*PartitionEventReader, error) {
	selectors := []topicoptions.ReadSelector{
		{Path: topicPath, Partitions: []int64{partition}},
	}

	baseEventReader, err := newTopicEventReader(consumer, selectors, ydbClient, logger)
	if err != nil {
		return nil, xerrors.Errorf("create base event reader error: %w", err)
	}

	return &PartitionEventReader{
		TopicEventReader: baseEventReader,
		partition:        partition,
		topicPath:        topicPath,
		consumer:         consumer,
		ydbClient:        ydbClient,
	}, nil
}
