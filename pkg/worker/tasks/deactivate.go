package tasks

import (
	"context"

	"github.com/transferia/transferia/internal/logger"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
)

// fast check whether deactivate may be skipped
func DeactivateNeeded(transfer model.Transfer) bool {
	if !providers.SourceIs[providers.Deactivator](&transfer) {
		return false
	}

	if transfer.SnapshotOnly() {
		switch transfer.Src.GetProviderType() {
		case provider_postgres.ProviderType, provider_mysql.ProviderType:
			return false
		default:
			return true
		}
	}

	return true
}

func Deactivate(ctx context.Context, cp coordinator.Coordinator, transfer model.Transfer, task model.TransferOperation, registry core_metrics.Registry) error {
	if !DeactivateNeeded(transfer) {
		return nil
	}
	deactivator, ok := providers.Source[providers.Deactivator](logger.Log, registry, cp, &transfer)
	if ok {
		return deactivator.Deactivate(ctx, &task)
	}
	return nil
}
