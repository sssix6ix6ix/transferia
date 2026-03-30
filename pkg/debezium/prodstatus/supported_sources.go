package prodstatus

import (
	"github.com/transferia/transferia/pkg/abstract"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
)

var supportedSources = map[string]bool{
	provider_postgres.ProviderType.Name(): true,
	provider_mysql.ProviderType.Name():    true,
	provider_ydb.ProviderType.Name():      true,
}

func IsSupportedSource(src string, _ abstract.TransferType) bool {
	return supportedSources[src]
}
