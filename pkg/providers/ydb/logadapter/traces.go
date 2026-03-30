package logadapter

import (
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_log "github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/library/go/core/log"
)

type Option = ydb_log.Option

func WithTraces(l log.Logger, d trace.Detailer, opts ...Option) ydb_go_sdk.Option {
	a := adapter{l: l}
	return ydb_go_sdk.MergeOptions(
		ydb_go_sdk.WithTraceDriver(ydb_log.Driver(a, d, opts...)),
		ydb_go_sdk.WithTraceTable(ydb_log.Table(a, d, opts...)),
		ydb_go_sdk.WithTraceScripting(ydb_log.Scripting(a, d, opts...)),
		ydb_go_sdk.WithTraceScheme(ydb_log.Scheme(a, d, opts...)),
		ydb_go_sdk.WithTraceCoordination(ydb_log.Coordination(a, d, opts...)),
		ydb_go_sdk.WithTraceRatelimiter(ydb_log.Ratelimiter(a, d, opts...)),
		ydb_go_sdk.WithTraceDiscovery(ydb_log.Discovery(a, d, opts...)),
		ydb_go_sdk.WithTraceTopic(ydb_log.Topic(a, d, opts...)),
		ydb_go_sdk.WithTraceDatabaseSQL(ydb_log.DatabaseSQL(a, d, opts...)),
	)
}
