package queue_to_s3_sink

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
)

const (
	topicMame = "testtopic"
)

func TestPartitionerNoEncoding(t *testing.T) {
	cfg := &s3_model.S3Destination{
		OutputFormat:   model.ParsingFormatJSON,
		OutputEncoding: s3_model.NoEncoding,
	}
	testItem := &changeitem.ChangeItem{
		QueueMessageMeta: changeitem.QueueMessageMeta{
			TopicName:    topicMame,
			Offset:       123,
			PartitionNum: 1,
		},
	}

	partitioner := PartitionerFactory(NewPartitionerConfig(cfg))
	path, err := partitioner.ConstructKey(testItem)
	require.NoError(t, err)
	expected := fmt.Sprintf("%s/partition=%d/%s+%d+%d.%s",
		testItem.QueueMessageMeta.TopicName,
		testItem.QueueMessageMeta.PartitionNum,
		testItem.QueueMessageMeta.TopicName,
		testItem.QueueMessageMeta.PartitionNum,
		testItem.QueueMessageMeta.Offset,
		strings.ToLower(string(cfg.OutputFormat)),
	)
	require.Equal(t, expected, path)

	defaultP, ok := partitioner.(*DefaultPartitioner)
	require.True(t, ok)
	require.Equal(t, len(expected), defaultP.calculateNameLength(testItem.QueueMessageMeta.Offset))
}

func TestPartitionerEncoding(t *testing.T) {
	cfg := &s3_model.S3Destination{
		OutputFormat:   model.ParsingFormatJSON,
		OutputEncoding: s3_model.GzipEncoding,
	}
	testItem := &changeitem.ChangeItem{
		QueueMessageMeta: changeitem.QueueMessageMeta{
			TopicName:    topicMame,
			Offset:       123,
			PartitionNum: 1,
		},
	}

	partitioner := PartitionerFactory(NewPartitionerConfig(cfg))
	path, err := partitioner.ConstructKey(testItem)
	require.NoError(t, err)
	expected := fmt.Sprintf("%s/partition=%d/%s+%d+%d.%s.gz",
		testItem.QueueMessageMeta.TopicName,
		testItem.QueueMessageMeta.PartitionNum,
		testItem.QueueMessageMeta.TopicName,
		testItem.QueueMessageMeta.PartitionNum,
		testItem.QueueMessageMeta.Offset,
		strings.ToLower(string(cfg.OutputFormat)),
	)
	require.Equal(t, expected, path)

	defaultP, ok := partitioner.(*DefaultPartitioner)
	require.True(t, ok)
	require.Equal(t, len(expected), defaultP.calculateNameLength(testItem.QueueMessageMeta.Offset))
}

func TestPartitionerIncorrectItem(t *testing.T) {
	cfg := &s3_model.S3Destination{
		OutputFormat:   model.ParsingFormatJSON,
		OutputEncoding: s3_model.NoEncoding,
	}
	testItem := &changeitem.ChangeItem{
		QueueMessageMeta: changeitem.QueueMessageMeta{
			TopicName:    "",
			Offset:       123,
			PartitionNum: 1,
		},
	}

	partitioner := PartitionerFactory(NewPartitionerConfig(cfg))
	_, err := partitioner.ConstructKey(testItem)
	require.Error(t, err)
}
