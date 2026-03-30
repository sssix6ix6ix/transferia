package source_factory

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSource(transfer *model.Transfer, lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator) (abstract.Source, error) {
	replicator, ok := providers.Source[providers.Replication](lgr, registry, cp, transfer)
	if !ok {
		lgr.Error("Unable to create source")
		return nil, xerrors.Errorf("unknown source: %s: %T", transfer.SrcType(), transfer.Src)
	}
	res, err := replicator.Source()
	if err != nil {
		return nil, xerrors.Errorf("unable to create %T: %w", transfer.Src, err)
	}
	return res, nil
}

func NewAsyncSource(transfer *model.Transfer, lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator) (abstract.QueueToS3Source, error) {
	replicator, ok := providers.Source[providers.PartitionableSource](lgr, registry, cp, transfer)
	if !ok {
		lgr.Error("Unable to create async source")
		return nil, xerrors.Errorf("unknown async source: %s: %T", transfer.SrcType(), transfer.Src)
	}
	// TODO fix in TM-9507
	res, err := replicator.PartitionSource(abstract.NewEmptyPartition())
	if err != nil {
		return nil, xerrors.Errorf("unable to create async %T: %w", transfer.Src, err)
	}
	return res, nil
}
