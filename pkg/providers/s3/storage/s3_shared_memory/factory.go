package s3_shared_memory

import (
	"context"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSharedMemory(
	ctx context.Context,
	logger log.Logger,
	registry core_metrics.Registry,
	transfer *model.Transfer,
	workerType abstract.WorkerType,
	cp coordinator.Coordinator,
	shardingContext []byte,
) (abstract.SharedMemory, error) {
	srcModel, ok := transfer.Src.(*s3_model.S3Source)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", transfer.Src)
	}

	switch workerType {
	case abstract.WorkerTypeMain:
		logger.Info("NewS3SharedMemory - factory calls NewS3SharedMemoryMainWorker")
		return NewS3SharedMemoryMainWorker(), nil
	case abstract.WorkerTypeSecondary:
		logger.Info("NewS3SharedMemory - factory calls NewS3SharedMemorySecondaryWorker")
		result, err := NewS3SharedMemorySecondaryWorker(ctx, logger, registry, srcModel, transfer.ID, transfer.Runtime, cp, shardingContext)
		if err != nil {
			return nil, xerrors.Errorf("unable to create NewS3SharedMemorySecondaryWorker, err: %w", err)
		}
		return result, nil
	case abstract.WorkerTypeSingleWorker:
		logger.Info("NewS3SharedMemory - factory calls NewS3SharedMemorySingleWorker")
		result, err := NewS3SharedMemorySingleWorker(ctx, logger, registry, srcModel)
		if err != nil {
			return nil, xerrors.Errorf("unable to create NewS3SharedMemorySingleWorker, err: %w", err)
		}
		return result, nil
	default:
		return nil, xerrors.Errorf("unknown worker type: %d", workerType)
	}
}
