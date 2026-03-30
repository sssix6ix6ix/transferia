package clickhouse

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/sink_factory"
)

func (p *Provider) loadClickHouseSchema() error {
	if _, ok := p.transfer.Src.(*clickhouse_model.ChSource); !ok {
		return nil
	}
	if _, ok := p.transfer.Dst.(*clickhouse_model.ChDestination); !ok {
		return nil
	}
	sink, err := sink_factory.MakeAsyncSink(p.transfer, new(model.TransferOperation), p.logger, p.registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return xerrors.Errorf("unable to make sinker: %w", err)
	}
	defer sink.Close()
	storage, err := p.Storage()
	if err != nil {
		return xerrors.Errorf("failed to resolve storage: %w", err)
	}
	defer storage.Close()
	tables, err := model.FilteredTableList(storage, p.transfer)
	if err != nil {
		return xerrors.Errorf("failed to list tables and their schemas: %w", err)
	}
	chStorage := storage.(*Storage)
	if err := chStorage.CopySchema(tables, abstract.PusherFromAsyncSink(sink)); err != nil {
		return xerrors.Errorf("unable to copy clickhouse schema: %w", err)
	}
	return nil
}
