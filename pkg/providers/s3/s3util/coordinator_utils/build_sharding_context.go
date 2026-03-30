package coordinator_utils

import (
	"context"
	"time"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3util_list "github.com/transferia/transferia/pkg/providers/s3/s3util/list"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/r_window"
	"go.ytsaurus.tech/library/go/core/log"
)

func BuildShardingContext(
	ctx context.Context,
	logger log.Logger,
	registry core_metrics.Registry,
	cfg *s3_model.S3Source,
) ([]byte, error) {
	startTime := time.Now()

	objects, err := s3util_list.ListAll(ctx, logger, registry, cfg)
	if err != nil {
		return nil, xerrors.Errorf("failed to list all objects, err: %w", err)
	}

	listTime := time.Since(startTime)

	resuit, err := r_window.NewRWindowFromFiles(cfg.OverlapDuration, listTime+cfg.OverlapDuration, objects)
	if err != nil {
		return nil, xerrors.Errorf("failed to create lr window, err: %w", err)
	}
	serialized, err := resuit.Serialize()
	if err != nil {
		return nil, xerrors.Errorf("failed to serialize resuit, err: %w", err)
	}
	return serialized, nil
}
