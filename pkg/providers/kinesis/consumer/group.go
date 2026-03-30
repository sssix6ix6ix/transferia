package consumer

import (
	"context"

	aws_kinesis "github.com/aws/aws-sdk-go/service/kinesis"
)

// Group interface used to manage which shard to process
type Group interface {
	Start(ctx context.Context, shardc chan *aws_kinesis.Shard)
	GetCheckpoint(streamName, shardID string) (string, error)
	SetCheckpoint(streamName, shardID, sequenceNumber string) error
}
