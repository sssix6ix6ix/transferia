package logadapter

import (
	"context"

	ydb_log "github.com/ydb-platform/ydb-go-sdk/v3/log"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ ydb_log.Logger = *new(adapter)

type adapter struct {
	l log.Logger
}

func (a adapter) Log(ctx context.Context, msg string, fields ...ydb_log.Field) {
	l := a.l
	for _, name := range ydb_log.NamesFromContext(ctx) {
		l = l.WithName(name)
	}

	switch ydb_log.LevelFromContext(ctx) {
	case ydb_log.TRACE:
		l.Debug(msg, ToCoreFields(fields)...) // replace 'trace' on 'debug' intentionally - bcs we use zap logger, zap don't log trace logs :-/
	case ydb_log.DEBUG:
		l.Debug(msg, ToCoreFields(fields)...)
	case ydb_log.INFO:
		l.Info(msg, ToCoreFields(fields)...)
	case ydb_log.WARN:
		l.Warn(msg, ToCoreFields(fields)...)
	case ydb_log.ERROR:
		l.Error(msg, ToCoreFields(fields)...)
	case ydb_log.FATAL:
		l.Fatal(msg, ToCoreFields(fields)...)
	default:
	}
}
